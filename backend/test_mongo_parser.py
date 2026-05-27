import sys
import os

# Add backend dir to path to import utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import parse_mongo_uri

def test_standard_uri():
    db = {
        "mongo_uri": "mongodb://user:pass@localhost:27017/testdb?authSource=admin",
        "database_name": "backupdb"
    }
    m = parse_mongo_uri(db)
    assert m["dbname"] == "testdb"
    assert m["username"] == "user"
    assert m["password"] == "pass"
    assert m["auth_source"] == "admin"
    assert "localhost:27017" in m["uri"]
    print("test_standard_uri passed!")

def test_path_options_uri():
    db = {
        "mongo_uri": "mongodb://mongodb-np.mongodb.svc.cluster.local:27017/authMechanism=SCRAM-SHA-256&authSource=admin",
        "database_name": "HotelStaticData"
    }
    m = parse_mongo_uri(db)
    assert m["dbname"] == "HotelStaticData"
    assert m["auth_mechanism"] == "SCRAM-SHA-256"
    assert m["auth_source"] == "admin"
    assert "HotelStaticData" in m["uri"]
    assert "authMechanism=SCRAM-SHA-256" in m["uri"]
    assert "authSource=admin" in m["uri"]
    print("test_path_options_uri passed!")

def test_special_char_password_uri():
    db = {
        "mongo_uri": "mongodb://root:6TXydaa3Xz!!qA4htWoJCn@mongodb-np.mongodb.svc.cluster.local:27017/authMechanism=SCRAM-SHA-256&authSource=admin",
        "database_name": "HotelStaticData"
    }
    m = parse_mongo_uri(db)
    assert m["dbname"] == "HotelStaticData"
    assert m["username"] == "root"
    assert m["password"] == "6TXydaa3Xz!!qA4htWoJCn"
    assert m["auth_mechanism"] == "SCRAM-SHA-256"
    assert m["auth_source"] == "admin"
    # Ensure double exclamation marks are URL-encoded in reconstructed URI
    assert "%21%21" in m["uri"]
    print("test_special_char_password_uri passed!")

def test_fallback_dict():
    db = {
        "host": "localhost",
        "port": 27017,
        "username": "root",
        "password": "my!password",
        "database_name": "mydb"
    }
    m = parse_mongo_uri(db)
    assert m["dbname"] == "mydb"
    assert m["username"] == "root"
    assert m["password"] == "my!password"
    assert "my%21password" in m["uri"]
    print("test_fallback_dict passed!")

if __name__ == "__main__":
    test_standard_uri()
    test_path_options_uri()
    test_special_char_password_uri()
    test_fallback_dict()
    print("All tests completed successfully!")
