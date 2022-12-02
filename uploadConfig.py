import firebase_admin
from firebase_admin import credentials, firestore
import json

def delete_collection(coll_ref, batch_size):
    docs = coll_ref.list_documents(page_size=batch_size)
    deleted = 0

    for doc in docs:
        print(f'Deleting doc {doc.id} => {doc.get().to_dict()}')
        doc.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)

def upload_json_file(channel_path, token_path):
    f_server = open(channel_path)
    f_token = open(token_path)
    server_data = json.load(f_server)
    token_data = json.load(f_token)
    f_server.close()
    f_token.close()
    for user in server_data:
        for server in server_data[user]:
            for channel in server_data[user][server]:
                doc_ref = db.collection("crawlerdb").document(channel)
                doc_ref.set(server_data[user][server][channel])
                doc_ref.set(token_data[user], merge=True)

if __name__ == "__main__":
    # Fetch the service account key JSON file contents
    cred = credentials.Certificate('')
    # Initialize the app with a service account, granting admin privileges
    firebase_admin.initialize_app(cred)

    db = firestore.client()

    # delete_collection(db.collection(u'user1'), 1)

    upload_json_file('./configs/user_server_channel_DO_NOT_EDIT.json', './configs/user_token.json')



