import asyncio
from typing import Any, List, Callable, Optional

class Action:
    BLOCK_TYPE = "Action"
    def __init__(self, name: str, parser: Optional[Callable] = None, retry_count: int = 0, retry_delay: int = 0, retry_on: Optional[Exception] = None):
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
        assert name != "Termination", "Action name cannot be 'Termination' (reserved for Termination action block)"
        assert name != "Save", "Action name cannot be 'Save' (reserved for Save action block)"
        
        self.__name__ = name
        self.retry_count: int = retry_count
        self.retry_delay: int = retry_delay
        self.retry_on: Optional[Exception] = retry_on

        if parser is not None:
            self.parser = parser
        else:
            self.parser: Callable = lambda x: x

        self.has_been_initialized_properly: bool = True

    async def forward(self, data: Any) -> Any:
        """
        Process the data. This method can be overridden by subclasses to implement custom behavior.

        Args:
            data (Any): The input data to process.

        Returns:
            Any: The processed data.
        """
        return data

    async def __call__(self, data: Any) -> Any:
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
                result = await self.forward(data)
                parsed_result = self.parser(result)
                return parsed_result
            except Exception as e:
                if self.retry_on is None or isinstance(e, self.retry_on):
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise e
        else:
            result = await self.forward(data)
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
    
    async def forward(self, data: Any) -> List[Any]:
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
    
    async def __call__(self, data: Any) -> Any:
        """
        Make the Action instance callable. It checks for proper initialization and calls the 'forward' method.

        Args:
            data (Any): The input data to process.

        Returns:
            Any: The processed data.
        """
        output = await self.forward(data)
        if not isinstance(output, list):
            raise ValueError("Split action expects list-type data.")
        return output


class Condition(Action):
    BLOCK_TYPE = "Condition"
    def __init__(self, name: str, condition: Callable[[Any], bool]):
        """
        Initialize a Condition action.

        Args:
            name (str): The name of the action.
            condition (Callable[[Any], bool]): A callable that takes data as input and returns a boolean.
        """
        super().__init__(name)
        self.condition = condition

    async def forward(self, data: Any) -> Any:
        """
        Process data based on a condition.

        Args:
            data (Any): The input data to process.

        Returns:
            Any: The processed data if condition is true, otherwise the unaltered data.
        """
        if self.condition(data):
            return await super().forward(data)
        else:
            return data

class Termination(Action):
    BLOCK_TYPE = "Termination"
    def __init__(self):
        """
        Initialize a Termination action.

        Args:
            name (str): The name of the action.
        """
        super().__init__("Termination")

    async def forward(self, data: Any) -> Any:
        """
        Termination action logic, if any.

        Args:
            data (Any): The input data to process.

        Returns:
            Any: Typically returns the data as is or a termination signal.
        """
        return None
