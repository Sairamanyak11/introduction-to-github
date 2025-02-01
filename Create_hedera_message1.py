import time
from datetime import datetime
from hedera import Client, ConsensusTopicCreateTransaction, ConsensusTopicMessageSubmitTransaction, ConsensusTopicMessageQuery
from hedera import PrivateKey

# Set up the Hedera client
client = Client.for_testnet()

# Set up the private key
private_key = PrivateKey.generate()
public_key = private_key.get_public_key()

# Create a new topic
topic_create_tx = ConsensusTopicCreateTransaction()
topic_create_tx.set_transaction_fee(100000000)
topic_create_tx.set_node_account_ids([client.get_network().get_node_account_ids()[0]])
topic_create_tx.set_admin_key(public_key)
topic_create_tx.set_submit_key(public_key)
topic_create_tx.set_auto_renew_period(604800)  # 7 days
topic_create_tx.set_auto_renew_account_id(client.get_operator_account_id())

# Sign the transaction
topic_create_tx.sign(private_key)

# Execute the transaction
topic_create_tx_response = topic_create_tx.execute(client)

# Get the topic ID
topic_id = topic_create_tx_response.get_topic_id()

print(f"Topic Created: {topic_id}")

# Send messages to the topic
messages = [
    "Hello, Hedera!",
    "Learning HCS",
    "Message 3"
]

print("Messages Sent:")
for i, message in enumerate(messages):
    # Create a new message transaction
    message_submit_tx = ConsensusTopicMessageSubmitTransaction()
    message_submit_tx.set_transaction_fee(100000000)
    message_submit_tx.set_node_account_ids([client.get_network().get_node_account_ids()[0]])
    message_submit_tx.set_topic_id(topic_id)
    message_submit_tx.set_message(message.encode("utf-8"))

    # Sign the transaction
    message_submit_tx.sign(private_key)

    # Execute the transaction
    message_submit_tx.execute(client)

    # Print the message
    print(f"{i+1}. \"{message}\" at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Wait for 1 second before sending the next message
    time.sleep(1)

# Retrieve messages from the topic
print("\nMessages Received:")
query = ConsensusTopicMessageQuery()
query.set_topic_id(topic_id)
query.set_start_consensus_timestamp(0)
query.set_end_consensus_timestamp(1000000000)

messages_response = query.execute(client)

for message in messages_response.get_messages():
    print(f"{messages_response.get_messages().index(message)+1}. \"{message.get_message().decode('utf-8')}\" at {datetime.fromtimestamp(message.get_consensus_timestamp().seconds)}")