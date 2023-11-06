import os

from dotenv import load_dotenv

from db_handler import serve

if __name__ == '__main__':
    load_dotenv()
    serve()