import psycopg2
import psycopg2.extras
import orjson
import requests
import yaml

def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def connect_to_database(db_config):
    return psycopg2.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        port=db_config['port'],
        cursor_factory=psycopg2.extras.DictCursor
    )

# 定期的に実行するためノートを昇順で取得する
def fetch_notes(cursor, last_indexed_id):
    query = (
        'SELECT "id", "userId", "userHost", "channelId", "cw", "text", "tags" FROM "note" '
        'WHERE ("note"."visibility" = \'public\' OR "note"."visibility" = \'home\') AND '
        '"note"."id" > %s '
        'ORDER BY "note"."id" ASC LIMIT 100000'
    )
    cursor.execute(query, (last_indexed_id,))
    return cursor.fetchall()

def send_notes_to_meilisearch(notes, meilisearch_config):
    if not notes:
        return
    # TODO httpsへの対応
    url = f"http://{meilisearch_config['host']}:{meilisearch_config['port']}/indexes/{meilisearch_config['index']}---notes/documents?primaryKey=id"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {meilisearch_config['api_key']}"}
    response = requests.post(url, data=orjson.dumps(notes), headers=headers)
    if response.status_code not in [200, 202]:
        raise Exception(f"Error sending data to Meilisearch: {response.text}")

def parse_aid(id):
    TIME2000 = 946684800000
    t = int(int(id[:8], 36) + TIME2000)
    return t

def format_note(note):
    return {
        'id': note['id'],
        'text': note['text'],
        'createdAt': parse_aid(note['id']),
        'userId': note['userId'],
        'userHost': note['userHost'],
        'channelId': note['channelId'],
        'cw': note['cw'],
        'tags': note['tags']
    }

def save_last_indexed_id(id):
    with open('last_indexed_id.txt', 'w') as file:
        file.write(str(id))

def load_last_indexed_id():
    try:
        with open('last_indexed_id.txt', 'r') as file:
            return int(file.read())
    except FileNotFoundError:
        return 0  # ファイルが存在しない場合は0から始める

def main():
    config = load_config('./.config/config.yml')
    meilisearch_config = config['meilisearch']

    last_indexed_id = load_last_indexed_id()

    db = connect_to_database(config['postgresql'])

    try:
        with db.cursor() as cursor:
            while True:
                fetched_notes = fetch_notes(cursor, last_indexed_id)
                if not fetched_notes:
                    break

                notes = [format_note(note) for note in fetched_notes]
                send_notes_to_meilisearch(notes, meilisearch_config)
                last_indexed_id = notes[-1]['id']
                save_last_indexed_id(last_indexed_id)

    finally:
        db.close()

if __name__ == "__main__":
    main()
