const { MongoClient } = require('mongodb');

const uri = "mongodb://localhost:27017";
const client = new MongoClient(uri);
const dbName = "plp_bookstore";

async function run() {
  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const books = db.collection("books");

    // Task 2: Basic CRUD Operations

    // 1. Find all books in a specific genre (e.g., "Fiction")
    const fictionBooks = await books.find({ genre: "Fiction" }).toArray();
    console.log("\n📚 Books in Fiction genre:", fictionBooks);

    // 2. Find books published after a certain year (e.g., 1950)
    const booksAfter1950 = await books.find({ published_year: { $gt: 1950 } }).toArray();
    console.log("\n📚 Books published after 1950:", booksAfter1950);

    // 3. Find books by a specific author (e.g., "George Orwell")
    const orwellBooks = await books.find({ author: "George Orwell" }).toArray();
    console.log("\n📚 Books by George Orwell:", orwellBooks);

    // 4. Update the price of a specific book (e.g., update '1984' price to 12.99)
    const updateResult = await books.updateOne(
      { title: "1984" },
      { $set: { price: 12.99 } }
    );
    console.log(`\n🔄 Updated ${updateResult.modifiedCount} document(s) - price of '1984'`);

    // 5. Delete a book by its title (e.g., delete 'Moby Dick')
    const deleteResult = await books.deleteOne({ title: "Moby Dick" });
    console.log(`\n🗑️ Deleted ${deleteResult.deletedCount} document(s) - 'Moby Dick'`);

    // Task 3: Advanced Queries

    // 1. Find books that are in stock AND published after 2010
    const recentInStockBooks = await books.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }).toArray();
    console.log("\n📚 Books in stock published after 2010:", recentInStockBooks);

    // 2. Use projection to return only title, author, and price fields
    const projectedBooks = await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray();
    console.log("\n📚 Books with projection (title, author, price):", projectedBooks);

    // 3. Sorting: Books by price ascending
    const booksByPriceAsc = await books.find().sort({ price: 1 }).toArray();
    console.log("\n📚 Books sorted by price (ascending):", booksByPriceAsc);

    // 4. Sorting: Books by price descending
    const booksByPriceDesc = await books.find().sort({ price: -1 }).toArray();
    console.log("\n📚 Books sorted by price (descending):", booksByPriceDesc);

    // 5. Pagination: Get page 1 (5 books per page)
    const pageSize = 5;
    const page1 = await books.find().skip(0).limit(pageSize).toArray();
    console.log("\n📚 Page 1 (5 books):", page1);

    // Pagination: Get page 2 (next 5 books)
    const page2 = await books.find().skip(pageSize).limit(pageSize).toArray();
    console.log("\n📚 Page 2 (next 5 books):", page2);

    // Task 4: Aggregation Pipeline

    // 1. Calculate average price of books by genre
    const avgPriceByGenre = await books.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } },
      { $sort: { avgPrice: -1 } }
    ]).toArray();
    console.log("\n📊 Average price by genre:", avgPriceByGenre);

    // 2. Find author with most books
    const topAuthor = await books.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log("\n🏆 Author with most books:", topAuthor);

    // 3. Group books by publication decade and count them
    const booksByDecade = await books.aggregate([
      {
        $group: {
          _id: {
            $concat: [
              { $toString: { $subtract: ["$published_year", { $mod: ["$published_year", 10] }] } },
              "s"
            ]
          },
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log("\n📊 Books grouped by publication decade:", booksByDecade);

    // Task 5: Indexing

    // 1. Create index on title field
    await books.createIndex({ title: 1 });
    console.log("\n✅ Index created on 'title' field");

    // 2. Create compound index on author and published_year
    await books.createIndex({ author: 1, published_year: -1 });
    console.log("✅ Compound index created on 'author' and 'published_year'");

    // 3. Use explain() to show query plan before and after index
    console.log("\n🔍 Explain query with index on title:");
    const explainTitle = await books.find({ title: "1984" }).explain("executionStats");
    console.log(JSON.stringify(explainTitle.executionStats, null, 2));

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
    console.log("\nConnection closed.");
  }
}

run();


