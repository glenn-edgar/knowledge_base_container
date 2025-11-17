from basic_contruct_db import BasicConstructDB


class SearchMemDB(BasicConstructDB):
    def __init__(self,host,port,dbname,user,password,table_name):
        BasicConstructDB.__init__(self,host,port,dbname,user,password,table_name)
        
        self.import_from_postgres(self.table_name)
        self.keys = self._generated_decoded_keys(self.data)
        
        
    def _generated_decoded_keys(self,data):
        self.kbs = {}
        self.labels = {}
        self.names = {}
        self.decoded_keys = {}
        for key in data.keys():
            self.decoded_keys[key] = key.split(".")
            kb = self.decoded_keys[key][0]
            label = self.decoded_keys[key][-2]
            name =  self.decoded_keys[key][-1]
            if kb not in self.kbs:
                self.kbs[kb] = []
            self.kbs[kb].append(key)
            if label not in self.labels:
                self.labels[label] = []
            self.labels[label].append(key)
            if name not in self.names:
                self.names[name] = []
            self.names[name].append(key)
 
        
    def clear_filters(self):
        """
        Clear all filters and reset the query state.
        """
        self.filter_results = self.data
        
        
        
    def search_kb(self, knowledge_base):
        """
        Search for rows matching the specified knowledge_base.
        
        Args:
            knowledge_base: The knowledge_base name to search for.
        """
        new_filter_results = {}
        for key in self.filter_results.keys():
            if key in self.kbs[knowledge_base]:
            
                new_filter_results[key] = self.filter_results[key]
        self.filter_results = new_filter_results
    
        return self.filter_results
        
    def search_label(self, label):
        """
        Search for rows matching the specified label.
        
        Args:
            label: The label value to match
        """
        new_filter_results = {}
        for key in self.filter_results.keys():
            if key in self.labels[label]:
                new_filter_results[key] = self.filter_results[key]
        self.filter_results = new_filter_results
        return self.filter_results
    
    def search_name(self, name):
        """
        Add a filter to search for rows matching the specified name.
        
        Args:
            name: The name value to match
        """
        new_filter_results = {}
        for key in self.filter_results.keys():
            if key in self.names[name]:
                new_filter_results[key] = self.filter_results[key]
        self.filter_results = new_filter_results
        return self.filter_results
    
    def search_property_key(self, data_key):
        new_filter_results = {}
        for key in self.filter_results.keys():
            value = self.data[key].data
            
            if data_key in value:
                new_filter_results[key] = self.filter_results[key]
        self.filter_results = new_filter_results
        return self.filter_results
    
    def search_property_value(self, data_key, data_value):
        """
        Add a filter to search for rows where the properties JSON field contains 
        the specified key with the specified value.
        
        Args:
            key: The JSON key to search
            value: The value to match for the key
        """
        new_filter_results = {}
        for key in self.filter_results.keys():
            value = self.data[key].data
            if data_key in value:
                 if data_value == value[data_key]:
                    new_filter_results[key] = self.filter_results[key]
        self.filter_results = new_filter_results
        return self.filter_results
    



    def search_starting_path(self, starting_path):
        """
        Version that returns filtered results without modifying the original filter_results.
        """
      
        if not isinstance(starting_path, str):
            raise ValueError("starting_path must be a string")
        
        new_filter_results = {}
        
        # Add starting path if it exists
        if starting_path in self.filter_results:
            new_filter_results[starting_path] = self.filter_results[starting_path]
        else:
            self.filter_results = {}
        
        # Get and add descendants
        try:
            starting_path_list = self.query_descendants(starting_path)
            if starting_path_list:
                for item in starting_path_list:
                    if item['path'] in self.filter_results:
                        new_filter_results[item['path']] = self.filter_results[item['path']]
        except Exception as e:
            print(f"Error querying descendants: {e}")
        
        self.filter_results = new_filter_results
        return new_filter_results
            
    def search_path(self,operator,starting_path):
        """
        Add a filter to search for rows matching the specified LTREE path expression.
        
        Args:
            operator: '@>', '<@', '~', '@@', '||'
           starting_path: The LTREE path expression to match
                Examples:
                - 'docs.technical' for exact match
                - 'docs.*' for all immediate children of docs
                - 'docs.*{1,3}' for children up to 3 levels deep
                - 'docs.technical.*' for all children of docs.technical
                - '*.technical.*' for any path containing 'technical'
        """
        search_results = self.query_by_operator(operator,starting_path)
        
        new_filter_results = {}
        for item in search_results:
            if item['path'] in self.filter_results:
                new_filter_results[item['path']] = self.filter_results[item['path']]
        self.filter_results = new_filter_results
        return self.filter_results
    

    def find_descriptions(self, key):
        """
        Extract description from properties field of query results.
        
        Args:
            key_data: Single row dictionary or list of row dictionaries
            
        Returns:
            list: List of dictionaries mapping path to description
        """
        if isinstance(key, list):
            key_data = key
        else:
            key_data = [key]
        
        return_values = {}
        
        for row_key in self.data.keys():
            row_data = self.data[row_key]
            data = row_data.data
            description = data.get('description', '') 
            return_values[row_key] = description
        return return_values
   
if __name__ == "__main__":
    password = input("Enter Postgres Password: ")
    kb = SearchMemDB(host="localhost", port=5432, dbname="knowledge_base", user="gedgar", password=password, table_name='composite_memory_kb')
    print("decoded_keys keys: ",kb.decoded_keys.keys())
    kb.clear_filters()
    print("----------------------------------")
    kb.search_kb("kb1")
    print("----------------------------------")
    print("search kb: ",kb.filter_results.keys())
    print("----------------------------------")
    kb.search_label("info1_link")
    print("search label: ",kb.filter_results.keys())
    kb.search_name("info1_name")
    print("----------------------------------")
    print("search name: ",kb.filter_results.keys())
    
    kb.clear_filters()
    print("search_property_key",kb.search_property_value('data','info1_data'))
    print("----------------------------------")
    kb.clear_filters()
    print("search property key: ",kb.search_property_key("data"))
    print("----------------------------------")
    
    kb.clear_filters()
    print("search_starting_path",kb.search_starting_path("kb2.header2_link.header2_name"))
    print("----------------------------------")
    kb.clear_filters()
    print("search_path",kb.search_path("~","kb2.**"))
    print("----------------------------------")
    kb.clear_filters()
    print("find_descriptions",kb.find_descriptions("kb2.header2_link.header2_name"))
    print("----------------------------------")
    
    