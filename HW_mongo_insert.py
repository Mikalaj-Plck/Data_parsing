from pymongo import MongoClient
import json

client = MongoClient('localhost', 27017)
db = client['shelf']
collection = db['books']

with open('Data_Parsing&Scrapping/GB_ParsingHTML/books_data.json', 'r') as f:
    books = json.load(f)
    total_books = len(books)

# Вставка пачками
batch_size = 100
total_inserted = 0

for i in range(0, total_books, batch_size):
    # Захватываем следующую пачку, включая последнюю неполную
    batch = books[i:i+batch_size]

    try:
        # Вставка пачки документов
        result = collection.insert_many(batch)

        # Подсчет вставленных документов
        batch_inserted = len(result.inserted_ids)
        total_inserted += batch_inserted
        print(f"Inserted batch: {batch_inserted} books. Total: {total_inserted}")

    except Exception as e:
        print(f"Error inserting batch: {e}")

print(f"Total books inserted: {total_inserted}")
print(f"Total books in original file: {total_books}")
