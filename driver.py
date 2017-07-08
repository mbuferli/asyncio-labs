import json
import shutil
DB_PATH = "data/one_producer_many_consumers.json"


def set_match_processed(match_id):
    """ Decrement expired_count to simulate processing """
    db = None
    new_expired_after = None
    with open(DB_PATH, 'r') as f:
        db = json.load(f)
        f.close()

    new_db = []
    with open(DB_PATH, 'w') as f:
        for row in db:
            if row['id'] == match_id:
                new_expired_after = row['expired_after'] - 1
                row['expired_after'] = new_expired_after
            new_db.append(row)
        f.seek(0)
        f.write(json.dumps(new_db, indent=True))
        f.close()
    return new_expired_after  # return, if negative don't re-Queue


def set_match_as_queued(match_id):
    """ Set match as queued """
    db = None
    with open(DB_PATH, 'r') as f:
        db = json.load(f)
        f.close()

    new_db = []
    with open(DB_PATH, 'w') as f:
        for row in db:
            if row['id'] == match_id:
                row['status'] = "QUEUED"
            new_db.append(row)
        f.seek(0)
        f.write(json.dumps(new_db, indent=True))
        f.close()


def get_waiting_matches():
    """ Get all waiting matches, return an array of ID """
    data = []
    with open(DB_PATH) as f:
        db = json.load(f)
        for row in db:
            if row['status'] == "WAITING":
                data.append(row['id'])
    f.close()
    return data


def reset_data():
    """ Reset json file. json file simulate DB data """
    shutil.copy2(
        'data/one_producer_many_consumers.ORIG.json',
        'data/one_producer_many_consumers.json'
    )
