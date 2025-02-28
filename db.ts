import { Pool } from "pg"
import { drizzle } from "drizzle-orm/node-postgres"
import { pgTable, serial, timestamp, varchar } from "drizzle-orm/pg-core"

// Create a PostgreSQL pool
const pool = new Pool({
  host: process.env.POSTGRES_HOST,
  port: Number.parseInt(process.env.POSTGRES_PORT || "5432"),
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
})

// Create the database client
export const db = drizzle(pool)

// Define the users table schema
export const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: varchar("email", { length: 255 }).notNull().unique(),
  passwordHash: varchar("password_hash", { length: 255 }).notNull(),
  name: varchar("name", { length: 255 }).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
})

// Define the user preferences table schema
export const userPreferences = pgTable("user_preferences", {
  id: serial("id").primaryKey(),
  userId: serial("user_id").references(() => users.id),
  symbol: varchar("symbol", { length: 10 }).notNull(),
  alertPriceAbove: varchar("alert_price_above", { length: 10 }),
  alertPriceBelow: varchar("alert_price_below", { length: 10 }),
  createdAt: timestamp("created_at").defaultNow(),
})

// Define the watchlists table schema
export const watchlists = pgTable("watchlists", {
  id: serial("id").primaryKey(),
  userId: serial("user_id").references(() => users.id),
  name: varchar("name", { length: 255 }).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
})

// Define the watchlist items table schema
export const watchlistItems = pgTable("watchlist_items", {
  id: serial("id").primaryKey(),
  watchlistId: serial("watchlist_id").references(() => watchlists.id),
  symbol: varchar("symbol", { length: 10 }).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
})

