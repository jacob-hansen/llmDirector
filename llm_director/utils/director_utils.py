from typing import Any, Dict, List

def flatten_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
        current_result["result"] = result["result"]
        final_results.append(current_result)
        if "chain" in result and result["chain"] is not None:
            final_results.extend(result["chain"])
    return final_results
