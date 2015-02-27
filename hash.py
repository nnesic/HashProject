import argparse
import random
import math
import sys


class Node(object):
    
    def __init__(self, position, parent=None):
        self.data = {}                  # Map that counts how many extents that map to a specific key are stored on this node
        self.replicated_data = {}       # Holds replicas from other nodes
        self.position = position        # Position on the ring
        self.writes_seen = 0            # During the testign phase, this keeps track of how many updates were made to extents on this node
        self.real_node = parent         # If the node is just a token, this points to the real node
        self.replicates = []            # List of nodes that this node replicates
        self.replicated_by = []         # List of nodes that hold this node's replicas. 

    def write(self):
        """ this method is used to access/modify extents during the testing phase."""
        if (self.real_node != None):
            self.real_node.write()
        else:
            self.writes_seen += 1

    def add_extent(self, key):
        if key not in self.data:
            self.data[key] = 0
        self.data[key] += 1

    def add_extent_replica(self, key):
        if key not in self.replicated_data:
            self.replicated_data[key] = 0
        self.replicated_data[key] += 1

    def clean_writes(self):
        self.writes_seen = 0

    def get_writes_seen(self):
        return self.writes_seen 

    def get_position(self):
        return self.position

    def is_Virtual(self):
        return self.real_node != None

    def data_count(self):
        """ count how many original data this node is storing"""
        count = 0
        for extent in self.data:
            count += self.data[extent]
        return count

    def replica_count(self):
        """ count how many replicated extents this node holds"""
        count = 0
        for extent in self.replicated_data:
            count += self.replicated_data[extent]
        return count

    def load_count(self):
        """ count total number of extents on this node"""
        return self.data_count() + self.replica_count()


    def __str__(self):
        ret = ""
        if self.is_Virtual():
            ret += "Node %d Token of %d \n" % (self.position, self.real_node.position)
        else:
            ret += "Node %d \n" % (self.position)
            ret += "Data count %d, Replica Count %d, Total Count %d \n" % (self.data_count(), self.replica_count(), self.load_count())
            ret += "Writes: %d \n" % self.get_writes_seen()
            ret += "Replicated By: "
            for node in self.replicated_by:
                ret += str(node.position) + " "
            ret += "\n"
            ret += "Replicates: "
            for node in self.replicates:
                ret += str(node.position) + " "
            ret += "\n"
            ret += "Data: %s \n" % str(self.data)
            ret += "Replicated data: %s \n" % str(self.replicated_data) 

        return ret

class Ring(object):
    #We should investigate the effects of the size of the ring too, perhaps
    def __init__(self, S, E, N, W, I, Sm, keys, tokens=0):
        self.nodes = []         # real and tokens
        self.real_nodes = []
        self.S = S
        self.E = E
        self.I = I
        self.W = W
        self.N = N
        self.Sm = Sm
        self.keys = keys
        self.tokens = tokens
        self.add_nodes = self.add_nodes_randomly  #method used to add nodes

        
        #self.add_nodes_randomly(3)
        self.initialize_ring()
        self.insert_extents()
        self.print_info()
        self.add_nodes_randomly(1)
        print "********************************"
        self.print_info()
        #import pdb; pdb.set_trace()
        #self.add_nodes(S)
        #self.evaluate()
        #self.add_servers_and_evaluate()


    def initialize_ring(self):
        for i in range(0, self.S):
            next_server = random.randint(0, self.keys-1)
            while (self.is_taken(next_server)):
                next_server = random.randint(0, self.keys-1)
            new_node = Node(next_server)
            self.real_nodes += [new_node]
            self.nodes += [new_node]
            for j in range (0, self.tokens):
                next_server = random.randint(0, self.keys-1)
                while (self.is_taken(next_server)):
                    next_server = random.randint(0, self.keys-1)
                self.nodes += [Node(next_server, new_node)]

            self.nodes.sort(key=lambda x: x.position, reverse=False)
            
        self.real_nodes.sort(key=lambda x: x.position, reverse=False)
        self.nodes.sort(key=lambda x: x.position, reverse=False)
        

        for i in range(0, len(self.real_nodes)):
            node = self.real_nodes[i]
            for j in range(1, self.N+1):
                index = (i +j) % len(self.real_nodes)
                node.replicated_by.insert(0, self.real_nodes[index])
                self.real_nodes[index].replicates.insert(0, node)



    def insert_extents(self):
        """ Inserts E extents into the system"""
        for i in range(0, self.E):
            self.write_extent(random.randint(0, self.keys-1))

    def add_nodes_randomly(self, numNodes):
        """ Adds numNodes nodes to the ring, distributed randomly """
        
        #if there are no nodes, initialize the ring
        if len(self.real_nodes) < self.S:
            self.initialize_ring()

        for i in range(0, numNodes):
            if (self.count_real_nodes() >= self.Sm):
                break

            # find the next free position for the node
            next_server = random.randint(0, self.keys-1)
            while (self.is_taken(next_server)):
                next_server = random.randint(0, self.keys-1)
            new_node = Node(next_server)

            # Find the node whose position is immediately smaller, to determine the range of extents to take over
            range_start = -1
            for i in range(0, len(self.nodes)):
                if self.nodes[i].position < new_node.position and (new_node.position - self.nodes[i].position) < (new_node.position - range_start):
                    range_start = self.nodes[i].position
            # if we found no applicable previous node, it means current node it the smallest positioned node, so its predecessor is the
            # last node on the ring
            if range_start == -1 and len(self.nodes) > 0:
                range_start = self.nodes[-1].position

            # go through all the real nodes, and check if their extents are anywhere in the range of the new node
            transfer_data = {}
            for node in self.real_nodes:
                for extent in node.data:
                    if (range_start > new_node.position and extent < new_node.position) or (extent <= new_node.position and extent > range_start):
                        if extent not in transfer_data:
                            transfer_data[extent] = 0
                        transfer_data[extent] += node.data[extent]

            # remove the transfer data from all existing nodes
            for node in self.real_nodes:
                for extent in transfer_data:
                    if extent in node.data:
                        del node.data[extent]
                    if extent in node.replicated_data:
                        del node.replicated_data[extent]
            
            # add the transfer data to the new node
            new_node.data = transfer_data
            
            self.nodes += [new_node]
            self.real_nodes += [new_node]
            self.real_nodes.sort(key=lambda x: x.position, reverse=False)
            self.nodes.sort(key=lambda x: x.position, reverse=False)
            self.redistribute_replicas(new_node)

            #add the tokens, if any
            for i in range (0, self.tokens):
                next_server = random.randint(0, self.keys-1)
                while (self.is_taken(next_server)):
                    next_server = random.randint(0, self.keys-1)
                self.nodes += [Node(next_server, new_node)]

            self.nodes.sort(key=lambda x: x.position, reverse=False)

        



    def redistribute_replicas(self, node):
        """ now a fun part : dealing with replicas. 
        the idea is: choose N adjacent nodes; and take over one of their "replicates" nodes; instead, assign them to replicate this node. """
        #find index of the node in the list
    
        node_index = -1
        for i in range(0, len(self.real_nodes)):
            if self.real_nodes[i].position == node.position:
                node_index = i
                break

        for i in range(1, self.N+1):
            next_node = self.real_nodes[(node_index + i) % len(self.real_nodes)]

            for replicated_node in next_node.replicates:
                if (replicated_node not in node.replicates):

                    node.replicates.insert(0, replicated_node)
                    node.replicated_by.insert(0, next_node)

                    replicated_node.replicated_by.remove(next_node)
                    replicated_node.replicated_by.insert(0, node)

                    next_node.replicates.remove(replicated_node)
                    next_node.replicates.insert(0, node)


                    # remove the replicated data from next node, and add it to the current node
                    for extent in replicated_node.data:
                        del next_node.replicated_data[extent]
                        node.replicated_data[extent] = replicated_node.data[extent]

                    # take over the replication data
                    for replicatee in node.replicates:
                        node.replicated_data.update(replicatee.data)
                    # replicate node's data into its replicator
                    for extent in node.data:
                        next_node.replicated_data[extent] = node.data[extent]
                    break


    def redistribute_replicas_perfectly(self):
        """ This is a simpler function that just wipes all the replicas, and distributes them to adjacent N nodes"""
        #clear the list
        for i in range(0, len(self.real_nodes)):
            node = self.real_nodes[i]
            node.replicated_by = []
            node.replicates = []
        for i in range(0, len(self.real_nodes)):
            node = self.real_nodes[i]
            for j in range(1, self.N+1):
                index = (i +j) % len(self.real_nodes)
                node.replicated_by += [self.real_nodes[index]]
                self.real_nodes[index].replicates += [node]

            # replicate new node's data to its replica set
            for replicator in node.replicated_by:
                replicator.replicated_data.update(node.data)

            # take over the replication data
            for replicatee in node.replicates:
                node.replicated_data.update(replicatee.data)


    def add_nodes_randomly_with_tokens(self, numNodes, tokens):
        """ Adds numNodes nodes to the ring with virtual nodes, distributed randomly """
        for i in range (0, numNodes):
            if (self.count_real_nodes() >= self.Sm):
                break
            next_server = random.randint(0, self.keys-1)
            while (self.is_taken(next_server)):
                next_server = random.randint(0, self.keys-1)
            real_node = Node(next_server)
            self.nodes += [real_node]
            self.real_nodes += [real_node]
            # Now add the tokens
            tokens = 100
            for i in range (0, tokens):
                next_server = random.randint(0, self.keys-1)
                while (self.is_taken(next_server)):
                    next_server = random.randint(0, self.keys-1)
                self.nodes += [Node(next_server, real_node)]


        self.nodes.sort(key=lambda x: x.position, reverse=False)
        

    def is_taken(self, position):
        for node in self.nodes:
            if node.position == position:
                return True
        return False 


    def write_extent(self, extent):
        """ Extent represented as its hash key number"""
       
        node_index = self.find_primary_node_for_key(extent)
        node = self.real_nodes[node_index] 
        node.add_extent(extent)

        for replica_node in node.replicated_by:
            replica_node.add_extent_replica(extent)

    def write_to_node(self, key_hash):
        node_index = find_primary_node_for_key(key_hash)
        # Write randomly to one of the node's replicas, or the node itself
        i = random.randint(0, self.N)
        if i < self.N:
            self.real_nodes[node_index].replicated_by[i].write()
        else:
            self.real_nodes[node_index].write()

    def find_primary_node_for_key(self, key_hash):
        min_distance = self.keys + 1
        node_index = -1
        #find the node at which to start writing
        for i in range (0, len(self.nodes)):
            if self.nodes[i].position > key_hash and (self.nodes[i].position - key_hash) < min_distance:
                min_distance = self.nodes[i].position - key_hash
                node_index = i
        
        # if the index is still not found, it means the node assigned should be the first in the circle
        if node_index == -1:
            node_index = 0

        if self.nodes[node_index].real_node != None:
            #if this is a token, we need to find the index of its parent
            parent_position = self.nodes[node_index].real_node.position
            for i in range (0, len(self.real_nodes)):
                if self.real_nodes[i].position == parent_position:
                    node_index = i
                    break
        return node_index


    def add_servers_and_evaluate(self):
        while len(self.real_nodes) < self.Sm:
            print "Adding servers"
            self.add_nodes(self.I)
            self.evaluate()


    def evaluate(self):
        for node in self.nodes:
            node.clean_writes()
        for i in range(0, self.W):
            key_hash = random.randint(0, self.keys-1)
            self.write_to_node(key_hash)
        self.print_info()


    def count_real_nodes(self):
        return len(self.real_nodes)


    def print_info(self):
        for node in self.nodes:
             print node
        #import pdb; pdb.set_trace()
        # dev = self.get_standard_deviation()
        # desired = self.W * self.N / len(self.real_nodes)
        # percentage_offset = dev * 100 / desired
        # print "Number of servers: %d Standard deviation: %d (%d%% off desired load)" % (self.count_real_nodes(), dev, percentage_offset)


    def get_standard_deviation(self):
        count = self.count_real_nodes()
        #mean
        mean = 0
        for node in self.real_nodes:
            mean += node.get_writes_seen()
        mean /= count
        #difference of squares
        o = 0
        for node in self.real_nodes:
            o += (node.get_writes_seen() - mean)**2
        o /= count
        return o**0.5



def main():

    parser = argparse.ArgumentParser(description="parse args")
    parser.add_argument('-S', type=int, help='Initial number of servers', default=10 )
    parser.add_argument('-E', type=int, help='Number of extents', default=10000)
    parser.add_argument('-N', type=int, help='Number of replicas', default=3)
    parser.add_argument('-W', type=int, help='Workload', default=1000000)
    parser.add_argument('-I', type=int, help='Server increment', default=5)
    parser.add_argument('-Sm', type=int, help='Maximum number of servers', default=30)
    parser.add_argument('-K', type=int, help='Number of keys on the ring', default=1000)


    args=parser.parse_args()

    #ring = Ring(args.S, args.E, args.N, args.W, args.I, args.Sm, args.K)
    ring = Ring(3, 10, 2, 100, 1, 20, 100, 2)
    #ring.evaluate()
    #ring.print_info()



if __name__ == '__main__':
    main()  
