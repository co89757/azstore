using System;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;
namespace azstore {
  public class TableQueryBuilder {
    private StringBuilder query;
    public TableQueryBuilder() {
      query = new StringBuilder();
    }

    public TableQueryBuilder PartitionKey(string pk) {
      query.AppendFormat("PartitionKey eq '{0}'", pk);
      return this;
    }
    public TableQueryBuilder RowKeyEquals(string rk) {
      query.AppendFormat("RowKey eq '{0}'", rk);
      return this;
    }

    public TableQueryBuilder RowKeyInRange(string lower, string upper) {
      if (lower == null && upper == null) {
        throw new ArgumentException("lower and upper bounds can't be both null");
      }
      if (lower == null) {
        query.AppendFormat("RowKey lt '{0}'", upper);
      } else if (upper == null) {
        query.AppendFormat("RowKey gt '{0}'", lower);
      } else {
        query.AppendFormat("RowKey gt '{0}' and RowKey lt '{1}'", lower, upper);
      }
      return this;
    }
    public TableQueryBuilder And() {
      query.Append(" and ");
      return this;
    }

    public TableQueryBuilder Or() {
      query.Append(" or ");
      return this;
    }

    public TableQueryBuilder GreaterThan(string propertyName, int value) {
      query.AppendFormat("{0} gt {1}", propertyName, value);
      return this;
    }

    public TableQueryBuilder GreaterThan(string propertyName, long value) {
      query.AppendFormat("{0} gt {1}", propertyName, value);
      return this;
    }

    public TableQueryBuilder GreaterThan(string propertyName, string value) {
      query.AppendFormat("{0} gt '{1}'", propertyName, value);
      return this;
    }

    public TableQueryBuilder GreaterThan(string propertyName, DateTimeOffset value) {
      var q = TableQuery.GenerateFilterConditionForDate(propertyName, QueryComparisons.GreaterThan, value);
      query.Append(q);
      return this;
    }

    public TableQueryBuilder LessThan(string propertyName, int value) {
      query.AppendFormat("{0} lt {1}", propertyName, value);
      return this;
    }

    public TableQueryBuilder LessThan(string propertyName, long value) {
      query.AppendFormat("{0} lt {1}", propertyName, value);
      return this;
    }
    public TableQueryBuilder LessThan(string propertyName, DateTimeOffset value) {
      var q = TableQuery.GenerateFilterConditionForDate(propertyName, QueryComparisons.LessThan, value);
      query.Append(q);
      return this;
    }

    public TableQueryBuilder LessThan(string propertyName, string value) {
      query.AppendFormat("{0} lt '{1}'", propertyName, value);
      return this;
    }
    public TableQueryBuilder IsTrue(string propertyName) {
      query.Append(propertyName);
      return this;
    }

    public TableQueryBuilder Not(string propertyName) {
      query.AppendFormat("not {0}", propertyName);
      return this;
    }

    public TableQueryBuilder Paren(TableQueryBuilder subquery) {
      query.AppendFormat("( {0} )", subquery.Build());
      return this;
    }

    public string Build() {
      return query.ToString();
    }
  }
}