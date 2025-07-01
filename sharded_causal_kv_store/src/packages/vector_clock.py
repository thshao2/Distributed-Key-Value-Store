"""
Implementation for Vector Clock Protocol:
Simple dict storing counter for each node_id

"""
class VectorClock:
  
  def __init__(self, node_id_list: list[str], clock = None):
    node_id_list = [str(x) for x in node_id_list]
    self.update_nodes(node_id_list=node_id_list, values=clock)

  def init_clock(self):
    for node_id in self.nodes:
      self.clock[str(node_id)] = 0
  
  def __str__(self):
    return str(self.clock)
  
  def to_dict(self):
    """Serializes into json, returning only the clock (dict type) of the Vector Clock"""
    return self.clock
    
  def __getitem__(self, index):
    print("Getting item of index", index)
    if str(index) not in self.nodes:
        raise KeyError(f"Node {index} is not in the clock's node list")
    return self.clock[str(index)]

  def __setitem__(self, key, value):
    if str(key) not in self.nodes:
        raise KeyError(f"Node {key} is not in the clock's node list")
    self.clock[str(key)] = value
  
  def __len__(self):
    return len(self.clock)
  
  def __copy__(self):
    return VectorClock(node_id_list=self.nodes.copy(), clock=self.clock.copy())
  
  def __eq__(self, clock2):
    if self is None or clock2 is None:
      return False
    return True if self.checkHappensBefore(clock2) == 2 else False
  
  def __lt__(self, clock2):
    """
    Checks if this vector clock is causally behind the passed in client vector clock.

    """
    return True if self.checkHappensBefore(clock2) == 1 else False
  
  def __gt__(self, clock2):
    """
    Checks if this vector clock is causally ahead the passed in client vector clock.

    """
    return True if self.checkHappensBefore(clock2) == -1 else False
  
  def __ge__(self, clock2):
    return self == clock2 or self > clock2

  def __le__(self, clock2):
    return self == clock2 or self < clock2
  
  def isConcurrent(self, clock2):
    return True if self.checkHappensBefore(clock2) == 0 else False

  def concurrent_break_ties(self, clock2):
    """If two clocks are concurrent, break ties between them. Only commit PUt/DELETE if msg's VC wins against local. 
    
    This logic could be modified to whatever, as long as it remains consistent throughout the system.

    Current Mechanism:
    1. Add up all the individual node's values, the clock with the larger sum wins (the node knows of more events).
    2. If the sums are equal, return the clock that has the min node with the largest value.

    Ex: 
    - [1 0 2] wins against [1 0 1] because sum 3 > 2
    - [2 0 1] wins against [1 0 2] because the sums are equal -> min node the largest value
    - [1 3 2] wins against [1 2 3] because the sume are equal -> min node has the largest value
    - [1 0 0 4] wins against [1 1 1 2] (different node_ids list) -> min node has the largest value

    Returns the winning clock object, or None if clocks are equal
    """
    if self == clock2:
      return None
        
    clock2: VectorClock = clock2
  
    # # Compute the sum and return if one is greater.
    # clock1_events_sum = sum(self.clock.values())
    # clock2_events_sum = sum(clock2.clock.values())
    # if clock1_events_sum > clock2_events_sum:
    #   return self
    # elif clock1_events_sum < clock2_events_sum:
    #   return clock2

    # Sums are equal; now break ties based on the smallest node id.
    # Create a sorted union of all node ids (as integers).
    all_node_ids = sorted(
        set([int(key) for key in self.clock.keys()]).union(set([int(key) for key in clock2.clock.keys()]))
    )

    print(f"all_node_ids: {all_node_ids}")

    for node_id in all_node_ids:
        node_str = str(node_id)
        in_self = node_str in self.clock
        in_clock2 = node_str in clock2.clock

        # If a node exists only in one clock (with a value greater than 0), that clock wins.
        if in_self and not in_clock2 and self.clock[node_str] > 0:
            return self
        if in_clock2 and not in_self and clock2.clock[node_str] > 0:
            return clock2

        # Both clocks have the node, so compare their values.
        val1 = self.clock[node_str]
        val2 = clock2.clock[node_str]
        if val1 > val2:
            return self
        elif val2 > val1:
            return clock2
    
    # If we reach here, something is really, really wrong.
    raise KeyError("In concurrent_break_ties(): You should not be here!")

    
  def pairwise_max(self, clock2):
    clock2: VectorClock = clock2

    newVC = VectorClock(self.nodes)
    newVC.update_view(clock2)

    for node_id in newVC.clock.keys():
      if (node_id in self.clock and node_id in clock2.clock):
        newVC.clock[node_id] = max(self.clock[node_id], clock2.clock[node_id])
      elif (node_id in self.clock):
        newVC.clock[node_id] = self.clock[node_id]
      else:
        newVC.clock[node_id] = clock2.clock[node_id]

    return newVC
 
  def update_nodes(self, node_id_list: list[str], values: dict = None):
      """
      DANGEROUS function: This function will overwrite the entire clock. Please think about
      why you need to call this function before doing so.
      
      Overwrites the list of nodes in the vector clock (with values of 0, or from the values parameter).
      
      Args:
          node_id_list (list[int]): The new list of node IDs.
          values (dict, optional): A dictionary with node IDs as keys and clock values.
              This will be used as the new vector clock (overwriting the old one).
      """
      self.nodes = node_id_list

      # Validate provided values if given.
      if values is not None:
          for key in values:
              if str(key) not in node_id_list:
                  raise KeyError(f"Node {key} in provided values is not in the new node list")
              
          for key in values.keys():
             if (str(key) not in node_id_list):
                raise KeyError(f"vector clock in values: {values} is missing node id {key}")

          # Replace the clock with the provided values.
          self.clock = values.copy()
      else:
        self.clock = {}
        self.init_clock()
  
  def update_view(self, clock2):
    """Updates clock to have the keys of clock2"""
    for node_id in clock2.clock:
        if node_id not in self.clock:
           self.nodes.append(node_id)
           self.clock[node_id] = 0
  
  def checkHappensBefore(self, clock2):
    """
        Checks if this vector clock happened before the passed in vector clock
        
        Returns:
             2 if this vector clock exactly equals the passed in vector clock
             1 if this vector clock happened before the passed in vector clock
             0 if both vector clocks are concurrent
            -1 if the passed in vector clock happened before this vector clock
    """
    clock2: VectorClock = clock2

    numLess = 0
    numMore = 0

    # Create a sorted union of all node ids (as integers).
    all_node_ids = sorted(
        set([int(key) for key in self.clock.keys()]).union(set([int(key) for key in clock2.clock.keys()]))
    )

    for node_id in all_node_ids:
      node_id = str(node_id)
      if (node_id in self.clock and node_id in clock2.clock):
        if self.clock[node_id] > clock2.clock[node_id]:
          numMore += 1
        elif self.clock[node_id] < clock2.clock[node_id]:
          numLess += 1
      elif (node_id in self.clock):
        if (self.clock[node_id] > 0):
          numMore += 1
      else:
        if (clock2.clock[node_id] > 0):
          numLess += 1

    if (numLess and not numMore):
       return 1
    
    if (numMore and numLess):
       return 0
    
    if (not numMore and not numLess):
       return 2
    
    return -1
    