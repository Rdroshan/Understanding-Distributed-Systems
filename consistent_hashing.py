"""
This is a basic implementation of consistent hashing to improve the understanding of the concept.
Currently will not implement virtual nodes in this.
Will not implement redistribution of data when a node is removed or added.
Considering cache misses and TTL will take care of that.
Also we're considering that max number of servers will be <360

key terminologies:
hash_server
hash_data
sorted_servers_in_ring: list --> This is to maintain the order and also to easily find
the successor node. Here we can use BST as well which will give a performance boost.
server_to_data_map: Dict[str, List] --> This is the database where data is stored
"""
from uuid import uuid4


class consistent_hashing:

	def __init__(self):
		self.server_to_data_map = {} # key will be server id, value will be list
		self.sorted_servers_in_ring = []
		self.__maintain_unique_servers_set = set()


	def hash_server(self, sid):
		val = hash(hash(sid)) % 360
		if val in self.__maintain_unique_servers_set:
			return self.hash_server(val)
		return val

	def hash_data(self, data):
		return hash(data) % 360	


	def add_data(self, data):
		if len(self.sorted_servers_in_ring) == 0:
			raise Exception("there are no servers to add data")

		hd = self.hash_data(data)
		succ_indx = self.__find_successor_server(hd)
		print("succ", succ_indx)
		succ_sid, _ = self.sorted_servers_in_ring[succ_indx]
		# Add data to the data store
		self.server_to_data_map[succ_sid].append(data)


	def delete_data(self, data):
		if len(self.sorted_servers_in_ring) == 0:
			raise Exception("there are no servers to delete data from")

		hd = self.hash_data(data)
		succ_indx= self.__find_successor_server(hd)
		succ_sid, _ = self.sorted_servers_in_ring[succ_indx]
		# print("server id from to remove", succ_sid)
		# Delete data from the data store
		server_data = self.server_to_data_map[succ_sid]
		for i in range(len(server_data)):
			if data == server_data[i]:
				self.server_to_data_map[succ_sid].pop(i)
				print(f"data {data} removed successfully")
				return
		print(f"no data found with val: {data}")



	def __find_successor_server(self, hash_val):
		index_predecessor = -1
		# binary search can also be used to get the index
		for i in range(len(self.sorted_servers_in_ring)):
			ring_sid, ring_val = self.sorted_servers_in_ring[i]
			if hash_val > ring_val:
				index_predecessor = i
			else:
				break
		if index_predecessor == -1:
			return 0
		return (index_predecessor + 1) % len(self.sorted_servers_in_ring)

	# [s1, s3, s2]
	# if new > s1 and < s3
	# then the new server s4 to be inserted at 1
	# if it was new > s2, then we need to append
	def add_server(self, sid):
		if self.server_to_data_map.get(sid) is not None:
			raise Exception(f"server with sid {sid} already added")
		val = self.hash_server(sid)

		self.server_to_data_map[sid] = []
		index_to_insert = -1
		# binary search can also be used to get the index
		# OR here we can just insert the element and do the sorting on the list based on value
		for i in range(len(self.sorted_servers_in_ring)):
			ring_sid, ring_val = self.sorted_servers_in_ring[i]
			if val > ring_val:
				index_to_insert = i
			else:
				break
		self.__maintain_unique_servers_set.add(val)
		if index_to_insert == len(self.sorted_servers_in_ring) - 1:
			self.sorted_servers_in_ring.append((sid, val))
		elif index_to_insert == -1:
			self.sorted_servers_in_ring.insert(0, (sid, val))
		else:
			self.sorted_servers_in_ring.insert(index_to_insert + 1, (sid, val))


	def remove_server(self, sid):
		pass

	def print_server_data_map_and_order(self):
		# This is just for verifying the data
		print("sorted servers in ring: ", self.sorted_servers_in_ring)
		print("servers mapping")
		for sid, val in self.server_to_data_map.items():
			print(sid, val)


ch = consistent_hashing()
print("intital servers")
ch.print_server_data_map_and_order()

# add server 1
s1 = str(uuid4())
ch.add_server(s1)
ch.print_server_data_map_and_order()

s2 = str(uuid4())
ch.add_server(s2)
ch.print_server_data_map_and_order()

d = "abc"
print("hash of ", d, ch.hash_data(d))
ch.add_data(d)


for i in ["cde", "efg", "jkl", "mno", "o"]:
	ch.add_data(i)
ch.print_server_data_map_and_order()


ch.add_data("roshan")
print("hash of ", "roshan", ch.hash_data("roshan"))
ch.print_server_data_map_and_order()

# ch.delete_data(d)
# ch.print_server_data_map_and_order()



