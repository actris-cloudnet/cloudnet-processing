from typing import Dict, List, Tuple

import numpy as np


def decode_bits(data: np.ndarray, format: List[Tuple[str, int]]) -> Dict[str, np.ndarray]:
    """
    Decode array of bit fields into decimals starting from the least-significant
    bit.

    Args:
        data: Array of bit fields.
        format: Tuple with name and bit size for each field. Names prefixed with
                underscore will be skipped.

    Returns:
        Dictionary from name to decoded values.
    """
    bits = data.copy()
    output = {}
    for name, size in format:
        if not name.startswith("_"):
            output[name] = bits & (2**size - 1)
        bits >>= size
    return output
