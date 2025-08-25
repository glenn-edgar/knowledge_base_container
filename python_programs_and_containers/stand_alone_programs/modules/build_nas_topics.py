

from building_blocks.libraries.nats.nats_key_store.nats_key_store import NATSKeyStore
from building_blocks.libraries.nats.nats_key_store.nats_key_store import KeyFormat

class BuildNasTopics:

    def __init__(self,system_name:str=None,site_name:str=None,site_properties:dict=None):
        self.namespace_list = ["system", system_name, "site", site_name]
        self.namespace = ".".join(self.namespace_list)
        self.nats_key_store = NATSKeyStore(host="localhost", port=4222, namespace=self.namespace, cache_size=100)
        success = await self.nats_key_store.store("site_properties", site_properties, KeyFormat.JSON)

    def build_nas_topics(self):
        pass


if __name__ == "__main__":
    build_nas_topics = BuildNasTopics(system_name="test", site_name="test", site_properties={"test": "test"})
    async def _main():
        user_data = await build_nas_topics.nats_key_store.retrieve("site_properties")
        print("user_data",user_data)
    asyncio.run(_main())