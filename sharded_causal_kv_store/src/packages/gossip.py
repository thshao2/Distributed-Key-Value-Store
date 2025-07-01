import asyncio
from contextlib import asynccontextmanager
from shared_data import SharedData
from helper import AsyncHelper, ReqHelper
import util
import httpx
import httpcore

class Gossip:
    @staticmethod
    @asynccontextmanager
    async def gossip():
        # Start the gossip loop as a background task.
        task = asyncio.create_task(Gossip._gossip_loop())
        try:
            # Yield control back to the app.
            yield
        finally:
            # When the app shuts down, cancel the gossip loop.
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                print("Gossip task cancelled gracefully.")
            except Exception as e:
                print("Error in gossip", e)

    @staticmethod
    async def _gossip_loop():
        # Index specifying which node we should talk to next
        ind = 0
        while True:
            print("This is the gossip protocol, ind is", ind)

            # No nodes in view or ind out of bound (view change occurred) - wait
            if len(SharedData.gossip_nodes) < 1 or ind >= len(SharedData.gossip_nodes):
                print("View change occurred, invalid ind - sleep a bit")
                ind = 0
                await asyncio.sleep(3)
                continue

            # Get current node to gossip to - if node is itself, we've gone a cycle of gossip, wait
            node = SharedData.gossip_nodes[ind]
            node_id = node["id"]
            node_addr = node["address"]
            if node_id == SharedData.NODE_IDENTIFIER:
                ind = (ind + 1) % len(SharedData.gossip_nodes)
                await asyncio.sleep(3)
                continue
            # Assemble payload & headers
            payload = {
                "kvstore": SharedData.kvstore,
                "causal-metadata": util.server_metadata_to_dict(SharedData.causal_data)
            }
            headers = ReqHelper.create_req_headers()
            
            # Send request
            print("Starting gossip to node ", node_id)
            try:
                res = await AsyncHelper.async_put(f"http://{node_addr}/copy", payload, headers=headers, timeout=4)
                body, status_code, headers = AsyncHelper.extract_res(res)
                print("Gossip to node ", node_id, "received. Status =", status_code, ", Body =", body)

            except TimeoutError:
                print("Gossip to node ", node_id, "timed out. v1")
                ind = (ind + 1) % len(SharedData.gossip_nodes)
                continue
            except httpx.ConnectTimeout:
                print("Gossip to node ", node_id, "timed out. v2")
                ind = (ind + 1) % len(SharedData.gossip_nodes)
                continue
            except httpcore.ConnectTimeout:
                print("Gossip to node ", node_id, "timed out. v3")
                ind = (ind + 1) % len(SharedData.gossip_nodes)
                continue
            except Exception as e:
                print("Gossip error after sending request:", e)

            # Extract kvs and metadata from response
            server_kvstore: dict = body.get("kvstore")
            server_metadata: dict = body.get("causal-metadata")
            
            # Merge kvs & causal metadata from response into our own
            if server_kvstore is not None and server_metadata is not None:
                server_metadata = util.dict_to_server_metadata(server_metadata)
                for key, server_dependencies in server_metadata.items():
                    # If the key doesn't belong in our shard, don't merge
                    if not util.key_in_current_shard(key):
                        continue

                    # convert dicts in dependencies to vector clocks
                    self_dependencies = SharedData.causal_data.get(key, {})
                    util.update_metadata(self_dependencies, server_dependencies, SharedData.kvstore, server_kvstore, key)
                    SharedData.causal_data[key] = self_dependencies

            print(f"Gossip to node {node_id} finished.")

            # Update ind
            ind = (ind + 1) % len(SharedData.gossip_nodes)

            # Wait 1 seconds before the next iteration.
            await asyncio.sleep(1)
