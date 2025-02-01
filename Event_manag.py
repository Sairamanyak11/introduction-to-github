from hedera import (
    Client,
    AccountId,
    PrivateKey,
    ConsensusTopicCreateTransaction,
    ConsensusMessageSubmitTransaction,
    ConsensusTopicId,
    TransactionId,
    Hbar,  # Ensure Hbar is imported
)
from datetime import datetime
import time

# Set up the Hedera client (Testnet)
client = Client.for_testnet()

# Set your Hedera account ID and private key
account_id = AccountId.from_string("your-account-id")  # Replace with your account ID
private_key = PrivateKey.from_string("your-private-key")  # Replace with your private key

client.set_operator(account_id, private_key)

# Step 1: Create a new consensus topic
def create_topic():
    # Create a new topic
    transaction = ConsensusTopicCreateTransaction() \
        .set_topic_admins([account_id]) \
        .set_max_transaction_fee(Hbar.from(2))  # Setting max fee to 2 Hbars
    
    # Execute the transaction
    response = transaction.execute(client)
    
    # Get the receipt for the transaction
    receipt = response.get_receipt(client)
    
    # The topic ID is returned in the receipt
    topic_id = receipt.topic_id
    print(f"Topic Created: {topic_id}")
    return topic_id

# Step 2: Send a message to the topic
def send_message_to_topic(topic_id, message):
    # Create a transaction to send a message to the topic
    transaction = ConsensusMessageSubmitTransaction() \
        .set_topic_id(topic_id) \
        .set_message(message) \
        .set_transaction_id(TransactionId.generate(account_id)) \
        .set_max_transaction_fee(Hbar.from(1))  # Setting max fee to 1 Hbar
    
    # Execute the transaction
    response = transaction.execute(client)
    
    # Get the receipt for the transaction
    receipt = response.get_receipt(client)
    print(f'Message Sent: "{message}" at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    return receipt

# Step 3: Retrieve messages from the topic (not typical for real-time, but for this demo we query with a delay)
def retrieve_messages_from_topic(topic_id):
    # Query the topic for messages (this assumes you know the message IDs or have the transaction IDs)
    # Hedera Consensus Service is event-driven and not pull-based, so you'd typically subscribe to the topic
    
    # For simplicity, simulate fetching messages in a demo way by using message logs or transaction IDs
    print("\nMessages Received:")
    print(f"1. 'Hello, Hedera!' at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"2. 'Learning HCS' at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"3. 'Message 3' at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Example usage
if name == "main":
    # Create a topic
    topic_id = create_topic()

    # Simulate sending messages to the topic
    send_message_to_topic(topic_id, "Hello, Hedera!")
    time.sleep(1)
    send_message_to_topic(topic_id, "Learning HCS")
    time.sleep(1)
    send_message_to_topic(topic_id, "Message 3")
    
    # Simulate receiving messages
    retrieve_messages_from_topic(topic_id)