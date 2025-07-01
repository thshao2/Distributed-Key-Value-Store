"""Implementation of FIFO Delivery Protocol."""
from threading import Lock, Condition

class FifoDelivery:
  # used for max_msg_num and curr_msg_num
  lock = Lock()
  cv = Condition(lock)

  # used_for_primary_curr_num
  primary_lock = Lock()
  primary_cv = Condition(primary_lock)

  max_msg_num = 0 # Next message number to give out to new request - used for primary
  
  # Used to determine when primary should deliver
  # Will be incremented after primary does a DELETE or PUT to their own KVS
  # This is to prevent a 2nd operation from being delivered before the 1st if 2nd gets all acks first
  primary_curr_num = 0 

  # Current request to deliver - used for backup
  # Key = sender's node identifer (sender will be the primary)
  # Val = current message to be delivered
  curr_msg_num = {} 

  # Get a new message number to attach to requests.
  # This function will be only used for the primary server at 
  #   the beginning of PUT and DELETE requests to 
  #   achieve a total order of delivery for primary and 
  #   FIFO delivery to backups
  @classmethod
  def get_new_msg_num(cls) -> int:
    with cls.lock:
      ret = cls.max_msg_num
      cls.max_msg_num += 1
      return ret
  
  # Blocking function that only returns when it's the message's turn to be delivered.
  # Use in combination with "finished_delivering" function below - 
  #     kind of like a critical region locked by a mutex. 
  # All delivery logic should happen in the "critical region".
  @classmethod
  def can_deliver(cls, sender_node_id: int, msg_id: int) -> None:
    print(f"Node {sender_node_id}'s message (id={msg_id}) waiting for delivery...")
    # Keep waiting until it's the message's turn to deliver.
    with cls.cv:
      if sender_node_id not in cls.curr_msg_num:
        cls.curr_msg_num[sender_node_id] = 0

      cls.cv.wait_for(lambda: msg_id == cls.curr_msg_num[sender_node_id])
      print(f"Node {sender_node_id}'s message (id={msg_id}) is ready for delivery.")
      return

  # Call once delivery is done to allow for next message to be delivered.
  # Check above function "can_deliver" for more info.
  @classmethod
  def finished_delivering(cls, sender_node_id: int) -> None:
    # Increment message number and notify other threads to start delivering.
    with cls.cv:
      cls.curr_msg_num[sender_node_id] += 1
      cls.cv.notify_all()
      print(f"Node {sender_node_id}'s msg_num incremented to {cls.curr_msg_num[sender_node_id]}")
      return

  # Same as can_deliver(), but this is for primary
  @classmethod
  def primary_can_deliver(cls, msg_id: int) -> None:
    print(f"Primary (id={msg_id}) waiting for delivery...")
    # Keep waiting until it's the message's turn to deliver.
    with cls.primary_cv:
      
      cls.primary_cv.wait_for(lambda: msg_id == cls.primary_curr_num)
      print(f"Primary (id={msg_id}) is ready for delivery.")
      return
  
  # Same as finished_delivering, but this is for primary
  @classmethod
  def primary_finished_delivering(cls) -> None:
    # Increment message number and notify other threads to start delivering.
    with cls.primary_cv:
      cls.primary_curr_num += 1
      cls.primary_cv.notify_all()
      print(f"Primary's curr_num incremented to {cls.primary_curr_num}")
      return