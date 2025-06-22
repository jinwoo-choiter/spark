
#include "spark/spark.h"

#include <sstream>
#include <stdexcept>

namespace spark {

Spark::Spark(const std::string& db_path) {
  if (sqlite3_open(db_path.c_str(), &db_) != SQLITE_OK)
    throw std::runtime_error("Cannot open database: " + std::string(sqlite3_errmsg(db_)));
}

Spark::~Spark() {
  if (db_) sqlite3_close(db_);
}

Spark::Spark(Spark&& other) noexcept : db_(other.db_) { other.db_ = nullptr; }

Spark& Spark::operator=(Spark&& other) noexcept {
  if (this != &other) {
    if (db_) sqlite3_close(db_);
    db_ = other.db_;
    other.db_ = nullptr;
  }
  return *this;
}

std::optional<std::string> Spark::QueryValue(const std::string& key) const {
  const char* sql = "SELECT value FROM parameters WHERE key = ? LIMIT 1;";
  sqlite3_stmt* stmt = nullptr;

  if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) return std::nullopt;

  sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);

  std::optional<std::string> result = std::nullopt;
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    result = std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
  }

  sqlite3_finalize(stmt);
  return result;
}

// GetValue template implementation
template <typename T>
std::optional<T> Spark::GetValue(const std::string& key) const {
  auto str_value = QueryValue(key);
  if (!str_value) return std::nullopt;

  T value;
  std::stringstream ss(*str_value);
  ss >> value;

  if (ss.fail() || !ss.eof()) return std::nullopt;

  return value;
}

// Template specializations
template <>
std::optional<std::string> Spark::GetValue(const std::string& key) const {
  return QueryValue(key);
}

// Explicit template instantiations
template std::optional<int> Spark::GetValue(const std::string& key) const;
template std::optional<long> Spark::GetValue(const std::string& key) const;
template std::optional<double> Spark::GetValue(const std::string& key) const;
template std::optional<float> Spark::GetValue(const std::string& key) const;
template std::optional<bool> Spark::GetValue(const std::string& key) const;

}  // namespace spark
