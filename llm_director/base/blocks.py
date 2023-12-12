import asyncio
from typing import Any, List, Callable, Optional, Dict
import time 

class Action:
    BLOCK_TYPE = "Action"
    def __init__(self, name: str, parser: Optional[Callable] = None, retry_count: Optional[int] = 0, retry_delay: Optional[int] = 0, retry_on: Optional[Exception] = None):
        """
        Initialize an Action object.

        Args:
            name (str): The name of the action.

        Attributes:
            __name__ (str): The name of the action.
            has_been_initialized_properly (bool): A flag indicating if the action is properly initialized.
            parser (Callable): A function to parse the input data, expects and returns a single argument.
            retry_count (int): The number of times to retry the action if it fails.
            retry_delay (int): The number of seconds to wait between retries.
            retry_on (Optional[Exception]): The exception to retry on. When None, all exceptions are retried.
        """
        assert name != "Termination" or self.BLOCK_TYPE == "Termination", "Action name cannot be 'Termination' (reserved for Termination action block)"
        assert name != "Save" or self.BLOCK_TYPE == "Save", "Action name cannot be 'Save' (reserved for Save action block)"
        
        self.__name__ = name
        self.retry_count: int = retry_count
        self.retry_delay: int = retry_delay
        self.retry_on: Optional[Exception] = retry_on

        if parser is not None:
            self.parser = parser
        else:
            self.parser: Callable = lambda x: x

        self.has_been_initialized_properly: bool = True

    async def forward(self, data: Any, args: Optional[Dict[str, Any]] = None) -> Any:
        """
        Process the data. This method can be overridden by subclasses to implement custom behavior.

        Args:
            data (Any): The input data to process.

        Returns:
            Any: The processed data.
        """
        return data

    async def __call__(self, data: Any, args: Optional[Dict[str, Any]] = None) -> Any:
        """
        Make the Action instance callable. It checks for proper initialization and calls the 'forward' method.

        Args:
            data (Any): The input data to process.

        Returns:
            Any: The processed data.

        Raises:
            Exception: If the action has not been initialized properly.
        """
        if not hasattr(self, "has_been_initialized_properly"):
            raise Exception(f"Action '{self.__name__}' has not been initialized properly, remember to call super().__init__(name) in the constructor")
        
        for _ in range(self.retry_count):
            try:
                result = await self.forward(data, args)
                parsed_result = self.parser(result)
                return parsed_result
            except Exception as e:
                if self.retry_on is None or isinstance(e, self.retry_on):
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise e
        else:
            result = await self.forward(data, args)
            parsed_result = self.parser(result)
            return parsed_result

class Split(Action):
    BLOCK_TYPE = "Split"
    def __init__(self, name):
        """
        Initialize a Split action. This action processes each element of an interable individually.

        The name of this action is set to 'Split' by default.
        """
        super().__init__(name)
    
    async def forward(self, data: Any, args: Optional[Dict[str, Any]] = None) -> List[Any]:
        """
        Process each element in the data list individually.

        Args:
            data (Any): The input data, expected to be a list.

        Returns:
            List[Any]: A list of results after processing each element in the data list.

        Raises:
            ValueError: If the input data is not a list.
        """
        return data
    
    async def __call__(self, data: Any, args: Optional[Dict[str, Any]] = None) -> Any:
        """
        Make the Action instance callable. It checks for proper initialization and calls the 'forward' method.

        Args:
            data (Any): The input data to process.

        Returns:
            Any: The processed data.
        """
        output = await self.forward(data, args)
        if not isinstance(output, list):
            raise ValueError("Split action expects list-type data.")
        return output


class Condition(Action):
    BLOCK_TYPE = "Condition"
    def __init__(self, name: str):
        """
        Initialize a Condition action.

        Args:
            name (str): The name of the action.
        """
        super().__init__(name)

    async def forward(self, data: Any, args: Optional[Dict[str, Any]] = None) -> Any:
        """
        Process data based on a condition.

        Args:
            data (Any): The input data to process.

        Returns:
            bool: True if the condition is met, False otherwise.
        """
        return True

    async def __call__(self, data: Any, args: Optional[Dict[str, Any]] = None) -> Any:
        """
        Make the Action instance callable. It checks for proper initialization and calls the 'forward' method.

        Args:
            data (Any): The input data to process.

        Returns:
            Any: The processed data.
        """
        return await self.forward(data, args)

class Save(Action):
    BLOCK_TYPE = "Save"
    def __init__(self, name: str, save_path: Optional[str] = None, include_timestamp: bool = False):
        """
        Initialize a Save action.

        Args:
            name (str): The name of the action.
            save_path (str, optional): The path to save the data to. Defaults to None.
        """
        super().__init__(name)
        self.history = []
        self.save_path = save_path
        self.include_timestamp = include_timestamp

    async def forward(self, data: Any, args: Optional[Dict[str, Any]] = None) -> Any:
        """
        Save the data to a file.

        Args:
            data (Any): The input data to process.

        Returns:
            Any: The unaltered data.
        """
        self.history.append(tuple([time.time(), data]))
        if self.save_path is not None:
            with open(self.save_path, "a") as f:
                if self.include_timestamp:
                    f.write(f"{time.time()}\t{data}\n")
                else:
                    f.write(f"{data}\n")
        return None
    
    def get_history(self, include_timestamp: Optional[bool] = None, delete_history: bool = False) -> List[Any]:
        """
        Get the history of the action.

        Args:
            include_timestamp (Optional[bool], optional): Whether to include the timestamp in the returned data. Defaults to None, which uses the class value of the 'include_timestamp' attribute.

        Returns:
            List[Any]: A list of all the data that has been processed by the action.
        """
        if (include_timestamp is not None and include_timestamp) or (include_timestamp is None and self.include_timestamp):
            return_list = [(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp)) + f".{str(timestamp).split('.')[1][:3]}Z", data) for timestamp, data in self.history]
        else:
            return_list = [data for _, data in self.history]
        if delete_history:
            self.history = []
        return return_list        

class Termination(Action):
    BLOCK_TYPE = "Termination"
    def __init__(self):
        """
        Initialize a Termination action.

        Args:
            name (str): The name of the action.
        """
        super().__init__("Termination")

    async def forward(self, data: Any, args: Optional[Dict[str, Any]] = None) -> Any:
        """
        Termination action logic, if any.

        Args:
            data (Any): The input data to process.

        Returns:
            Any: Typically returns the data as is or a termination signal.
        """
        return None
