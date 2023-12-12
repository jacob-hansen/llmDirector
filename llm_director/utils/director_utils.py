from typing import Any, Dict, List, Callable
from base.blocks import Action

def flatten_results_func(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten the results of a director call.

    Args:
        results (List[Dict[str, Any]]): The results of a director call.

    Returns:
        List[Dict[str, Any]]: The flattened results.
    """
    final_results = []
    for result in results:
        current_result = {}
        current_result["event_name"] = result["event_name"]
        if "result" in result:
            current_result["result"] = result["result"]
            final_results.append(current_result)
        if "chain" in result and result["chain"] is not None:
            final_results.extend(result["chain"])
    return final_results


EXCLUSION_LISTENERS = ["Termination", "Save"]

def return_endpoints(return_results: List[Dict[str, Any]], listeners: Dict[str,List[Action]]) -> bool:
    """
    Recursive algorithm to extract only the endpoints from a nested set of director results.

    Args:
        return_results (List[Dict[str, Any]]): The results of a director call.
        listeners (Callable): The listeners to check.

    Returns:
        new_return_results (List[Dict[str, Any]]): The endpoints of the director call.
    """
    new_return_results = []
    if return_results is None:
        return None
    for result in return_results:
        event_name = result["event_name"]
        # 1. Event does not have any listeners
        if event_name not in listeners and "chain" not in result:
            new_return_results.append(result)
        # 2. Event has listeners, but has a chain. SHOULD NOT HAPPEN EVER!!! But easy to handle.
        elif event_name not in listeners:
            new_result = {"event_name": event_name, "result": result["result"]}
            new_result["chain"] = return_endpoints(result["chain"], listeners)
            new_return_results.append(new_result)
        # 3. Event has listeners, and thus a chain. Filter listeners by EXLUSION_LISTENERS and check if any remain.
        else:
            # 3a. No listeners remain, so add the result to the new_return_results
            if len([listener for listener in listeners[event_name] if listener.BLOCK_TYPE not in EXCLUSION_LISTENERS]) == 0:
                result.pop("chain", None)
                new_return_results.append(result)
            else:
                # 3b. Listeners remain, so recursively call return_endpoints on the chain. Not including the result.
                new_result = {"event_name": event_name,}
                if "chain" in result:
                    new_result["chain"] = return_endpoints(result["chain"], listeners)
                new_return_results.append(new_result)
    return new_return_results