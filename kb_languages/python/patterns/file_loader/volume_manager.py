class Volume_Definition_Table:
    def __init__(self, postgres_connector):
        self.conn = postgres_connector
        self._create_table()
        self.existing_volumes = self._get_existing_volumes()
    
    def _create_table(self):
        """Create the volume_definition_table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS volume_definition_table (
            id SERIAL PRIMARY KEY,
            volume_name VARCHAR(255) UNIQUE NOT NULL,
            volume_path TEXT,
            volume_description TEXT
        );
        """
        with self.conn.cursor() as cursor:
            cursor.execute(create_table_sql)
        self.conn.commit()
    
    def _get_existing_volumes(self):
        """Retrieve all existing volume names from the database"""
        select_sql = "SELECT volume_name FROM volume_definition_table;"
        with self.conn.cursor() as cursor:
            cursor.execute(select_sql)
            results = cursor.fetchall()
        return {row[0] for row in results}
    
    def check_volume(self, volume_name):
        """Check if a volume exists in the database"""
        return volume_name in self.existing_volumes
    
    def add_volume(self, volume_name, volume_path, volume_description):
        """Add a new volume to the database"""
        if self.check_volume(volume_name):
            raise ValueError(f"Volume '{volume_name}' already exists")
        
        insert_sql = """
        INSERT INTO volume_definition_table (volume_name, volume_path, volume_description)
        VALUES (%s, %s, %s);
        """
        with self.conn.cursor() as cursor:
            cursor.execute(insert_sql, (volume_name, volume_path, volume_description))
        self.conn.commit()
        self.existing_volumes.add(volume_name)
    
    def delete_volume(self, volume_name):
        """Delete a volume from the database"""
        if not self.check_volume(volume_name):
            raise ValueError(f"Volume '{volume_name}' does not exist")
        
        delete_sql = "DELETE FROM volume_definition_table WHERE volume_name = %s;"
        with self.conn.cursor() as cursor:
            cursor.execute(delete_sql, (volume_name,))
        self.conn.commit()
        self.existing_volumes.discard(volume_name)
    
    def delete_all_volumes(self):
        """Delete all volumes from the database"""
        delete_all_sql = "DELETE FROM volume_definition_table;"
        with self.conn.cursor() as cursor:
            cursor.execute(delete_all_sql)
        self.conn.commit()
        self.existing_volumes.clear()
    
    def get_all_volumes(self):
        """Retrieve all volumes from the database"""
        select_all_sql = """
        SELECT id, volume_name, volume_path, volume_description 
        FROM volume_definition_table 
        ORDER BY id;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(select_all_sql)
            results = cursor.fetchall()
        
        return [
            {
                'id': row[0],
                'volume_name': row[1],
                'volume_path': row[2],
                'volume_description': row[3]
            }
            for row in results
        ]


