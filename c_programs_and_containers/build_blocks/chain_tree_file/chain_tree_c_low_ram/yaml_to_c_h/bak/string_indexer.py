class StringIndexer:
    def __init__(self):
        self.string_dict = {}
        self.verify_dict = {}
    
    def add_string(self, string):
        """
        Records the string in the dictionary with value as current dictionary length.
        Returns the assigned number.
        """
        if string not in self.string_dict:
            index = len(self.string_dict)
            self.string_dict[string] = index
            self.verify_dict[string] = False
            return index
        return self.string_dict[string]
    
    def verify_string(self, string):
        """
        Verifies the string in the dictionary with value as current dictionary length.
        Returns the assigned number.
        """
        if string not in self.verify_dict:
            raise ValueError(f"String {string} not found in verify dictionary")
        if self.verify_dict[string] == False:
            self.verify_dict[string] = True
            return True
        else:
            raise ValueError(f"String {string} already verified")
        
    def get_index(self, string):
        """
        Returns the number associated with the string if found, None otherwise.
        """
        return self.string_dict.get(string, None)
    
    def get_dictionary(self):
        """
        Returns the dictionary.
        """
        return self.string_dict


# Example usage:
if __name__ == "__main__":
    indexer = StringIndexer()
    
    # Add some strings
    print(indexer.add_string("hello"))    # Returns 0
    print(indexer.add_string("world"))    # Returns 1
    print(indexer.add_string("python"))   # Returns 2
    
    # Get index of existing string
    result = indexer.get_index("world")
    print(result)                          # Returns 1
    
    # Try to get index of non-existent string
    result = indexer.get_index("java")
    print(result)                          # Returns None
    
    # Get the full dictionary
    print(indexer.get_dictionary())       # {'hello': 0, 'world': 1, 'python': 2}