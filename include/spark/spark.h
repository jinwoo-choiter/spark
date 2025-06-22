#ifndef SPARK_SPARK_H_
#define SPARK_SPARK_H_

#include <optional>
#include <string>

#include <sqlite3.h>

namespace spark {

class Spark {
 public:
  explicit Spark(const std::string& db_path);
  ~Spark();

  Spark(const Spark&) = delete;
  Spark& operator=(const Spark&) = delete;
  Spark(Spark&&) noexcept;
  Spark& operator=(Spark&&) noexcept;

  template <typename T>
  std::optional<T> GetValue(const std::string& key) const;

 private:
  std::optional<std::string> QueryValue(const std::string& key) const;

  sqlite3* db_ = nullptr;
};

}  // namespace spark

#endif  // SPARK_SPARK_H_
