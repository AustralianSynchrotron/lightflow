
def find_indices(lst, element):
    """ Returns the indices for all occurrences of 'element' in 'lst'.

    Args:
        lst (list): List to search.
        element:  Element to find.

    Returns:
        list: List of indices or values
    """
    result = []
    offset = -1
    while True:
        try:
            offset = lst.index(element, offset+1)
        except ValueError:
            return result
        result.append(offset)
