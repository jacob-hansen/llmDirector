import asyncio
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Tuple, Optional
from utils.director_utils import flatten_results_func, return_endpoints
from fastapi import FastAPI, Request
import uvicorn
import requests

class Director:
    def __init__(self, max_concurrent_actions: int = 100, max_logs: int = 1000000, depth_first: bool = False):
        """
        Initialize a Director object.

        Args:
            max_concurrent_actions (int): Maximum number of concurrent actions.
            max_logs (int): Maximum number of log entries to store.
            depth_first (bool): Flag to determine if depth-first execution should be used.

        Attributes:
            listeners (Dict[str, List[Callable]]): A dictionary mapping event names to listener functions.
            logs (deque): A deque for storing log messages with a maximum length.
            semaphore (asyncio.Semaphore): Semaphore to limit the number of concurrent actions.
            depth_first (bool): Flag indicating if depth-first execution is enabled, triggering subsequent events before the current event completes on all listeners.
            actions (Dict[str, Callable]): A dictionary mapping action names to their instances.
            api_app (FastAPI): A FastAPI instance for exposing the Director API.
        """
        self.listeners: Dict[str, List[Callable]] = {}
        self.logs: deque = deque(maxlen=max_logs)
        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(max_concurrent_actions)
        self.depth_first: bool = depth_first
        self.actions: Dict[str, Callable] = {}
        self.api_app = FastAPI()

    def subscribe(self, event_name: str, listener: Callable):
        """
        Subscribe a listener to an event.

        Args:
            event_name (str): The name of the event to subscribe to.
            listener (Callable): The listener function to subscribe.
            duplicate (bool): Flag to allow duplicate actions.

        Raises:
            Exception: If the action already exists and duplicate is False.
        """
        action_name = listener.__name__
        # check for duplication (where connection already exists)
        if action_name in self.actions and listener == self.actions[action_name] and event_name in self.listeners and listener in self.listeners[event_name]:
            raise Exception(f"Action '{action_name}' already exists and is subscribed to '{event_name}'")

        if action_name in self.actions and listener != self.actions[action_name]:
            raise Exception(f"Action '{action_name}' already exists. Consider renaming the action, using add_subscription for existing block, or remove and add the action again.")
        
        self.actions[action_name] = listener
        
        if event_name not in self.listeners:
            self.listeners[event_name] = []
            
        self.listeners[event_name].append(listener)
    
    def add_subscription(self, event_name: str, action_name: str):
        """
        Add a subscription to an event.

        Args:
            event_name (str): The name of the event.
            action_name (str): The name of the action.

        Raises:
            AssertionError: If the action does not exist.
        """
        assert action_name in self.actions, f"Action '{action_name}' does not exist, please subscribe it first"
        listener = self.actions[action_name]
        if event_name not in self.listeners:
            self.listeners[event_name] = []
        self.listeners[event_name].append(listener)
    
    def remove_subscription(self, event_name: str, action_name: str):
        """
        Remove a subscription from an event.

        Args:
            event_name (str): The name of the event.
            action_name (str): The name of the action.

        Raises:
            AssertionError: If the action does not exist.
        """
        assert action_name in self.actions, f"Action '{action_name}' does not exist, please subscribe it first"
        listener = self.actions[action_name]
        if event_name in self.listeners:
            self.listeners[event_name].remove(listener)

    def get_subscriptions(self, event_name: str) -> List[str]:
        """
        Get all subscriptions for a given event.

        Args:
            event_name (str): The name of the event.

        Returns:
            List[str]: A list of action names subscribed to the event.
        """
        if event_name in self.listeners:
            return [listener.__name__ for listener in self.listeners[event_name]]
        else:
            return []

    async def __call__(self, event_name: str, data: Any, args: Optional[Dict[str, Any]] = None, flatten_results: bool = False, completion_results: bool = False) -> List[Dict[str, Any]]:
        """
        Make the Director instance callable. It processes the events and manages the listeners.

        Args:
            event_name (str): The name of the event to process.
            data (Any): The data to be passed to the listeners.
            args (Optional[Any]): Optional arguments to pass to the listeners.
            flatten_results (bool): Flag to determine if results should be flattened.
            completion_results (bool): Flag to return only the results of the final event.

        Returns:
            List[Dict[str, Any]]: A list of results from the event listeners.
        """
        kwargs = {"flatten_results": flatten_results, "completion_results": completion_results}

        # Limit the number of concurrent actions with a semaphore
        async with self.semaphore:
            return_results = []
            if event_name in self.listeners:
                self.log(f"Processing events for '{event_name}'")
                events = []
                for listener in self.listeners[event_name]:                    
                    action_completion = asyncio.create_task(self._handle_listener(listener, data, args=args, kwargs=kwargs))

                    # If depth-first execution is enabled, wait for the action to complete before triggering the next event
                    if self.depth_first:
                        data = await action_completion
                        chain_results = data[1]
                        for process in chain_results:
                            output = await process
                            result = {"event_name": listener.__name__, "result": data[0], "chain": output}
                            return_results.append(result)
                        continue
                    else:
                        events.append((listener.__name__, action_completion))
                
                # events is non-empty only when depth-first execution is disabled
                for event in events:
                    data = await event[1]
                    chain_results = data[1]
                    for process in chain_results:
                        output = await process
                        result = {"event_name": event[0], "result": data[0], "chain": output}
                        return_results.append(result)

                self.log(f"Events for '{event_name}' completed")

                
                # if there is listeners and completion_results is True, return only the chain
                if completion_results:
                    return_results = return_endpoints(return_results, self.listeners)
                if flatten_results: 
                    result = flatten_results_func(return_results)
                    return result
                return return_results
            else: 
                self.log(f"No listeners for '{event_name}'")
                return None
    
    async def batch_run(self, calls: List[Tuple[str, Any]]) -> List[Any]:
        """
        Run a batch of actions concurrently and wait for all to complete.

        Args:
            calls (List[Tuple[str, Any]]): A list of tuples, each containing an event name and the associated data.

        Returns:
            List[Any]: A list containing the results of all the actions.
        """
        coroutines = [self.__call__(event_name, data) for event_name, data in calls]
        return await asyncio.gather(*coroutines)

    async def _handle_listener(self, listener: Callable, data: Any, args: Optional[Dict[str, Any]] = None, kwargs: Dict[str, Any] = {}) -> Tuple[Any, List[Dict[str, Any]]]:
        """
        Handle a listener function, processing its result and triggering subsequent events.

        Args:
            listener (Callable): The listener function to handle.
            data (Any): The data to pass to the listener.

        Returns:
            Tuple(Any, List[Dict[str, Any]]): A tuple containing the result of the listener and the chain of subsequent event results.
        """
        result = await listener(data, args=args)
        next_event_name = listener.__name__
        
        if listener.BLOCK_TYPE == "Split":
            # trigger next event for each result
            self.log(f"Splitting '{next_event_name}'")
            chain = [self.__call__(next_event_name, r, args=args, **kwargs) for r in result]
        elif listener.BLOCK_TYPE == "Condition":
            # trigger next event if condition is met
            if result:
                self.log(f"Condition '{next_event_name}' met")
                chain = [self.__call__(next_event_name, args=args, data, **kwargs),]
            else:
                self.log(f"Condition '{next_event_name}' failed")
                chain = []
            return (data, chain)
        else:
            # trigger next event once on the result
            chain = [self.__call__(next_event_name, result, args=args, **kwargs),]
        return (result, chain)

    def log(self, message: str):
        """
        Log a message to the director's logs.

        Args:
            message (str): The message to log.
        """
        self.logs.append(message)

    def create_api_routes(self):
        """
        Create FastAPI routes for each listener in the Director.
        """
        for event_name, listeners in self.listeners.items():
            for listener in listeners:
                if hasattr(listener, "BLOCK_TYPE") and listener.BLOCK_TYPE == "Save":
                    self._create_save_route(event_name, listener)

                @self.api_app.post(f"/api/{event_name}")  # Create a POST route for each event
                async def route(request: Request, event_name=event_name):
                    data = await request.json()
                    return await self.__call__(event_name, data)

    def _create_save_route(self, event_name: str, listener: Any):
        """
        Create a specific route for a Save block to handle history data retrieval.

        Args:
            event_name (str): The event name associated with the Save block.
            listener (Any): The listener object representing the Save block.
        """
        @self.api_app.get(f"/api/{event_name}/history")
        async def history_route(include_timestamp: Optional[bool] = None, delete_history: bool = False):
            return listener.get_history(include_timestamp, delete_history)
    
    def run_api(self, host: str = "0.0.0.0", port: int = 8000):
        """
        Run the FastAPI server with the created routes.

        WARNING!!! This method is blocking and should be called last in the script.

        Args:
            host (str): The host address on which to run the API server.
            port (int): The port on which to run the API server.
        """
        self.create_api_routes()
        uvicorn.run(self.api_app, host=host, port=port)