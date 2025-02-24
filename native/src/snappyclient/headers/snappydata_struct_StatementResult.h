/**
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */

#ifndef SNAPPYDATA_STRUCT_STATEMENTRESULT_H
#define SNAPPYDATA_STRUCT_STATEMENTRESULT_H


#include "snappydata_struct_Decimal.h"
#include "snappydata_struct_BlobChunk.h"
#include "snappydata_struct_ClobChunk.h"
#include "snappydata_struct_TransactionXid.h"
#include "snappydata_struct_ServiceMetaData.h"
#include "snappydata_struct_ServiceMetaDataArgs.h"
#include "snappydata_struct_OpenConnectionArgs.h"
#include "snappydata_struct_ConnectionProperties.h"
#include "snappydata_struct_HostAddress.h"
#include "snappydata_struct_SnappyExceptionData.h"
#include "snappydata_struct_StatementAttrs.h"
#include "snappydata_struct_ColumnValue.h"
#include "snappydata_struct_ColumnDescriptor.h"
#include "snappydata_struct_Row.h"
#include "snappydata_struct_OutputParameter.h"
#include "snappydata_struct_RowSet.h"
#include "snappydata_struct_PrepareResult.h"
#include "snappydata_struct_UpdateResult.h"

#include "snappydata_types.h"

namespace io { namespace snappydata { namespace thrift {

typedef struct _StatementResult__isset {
  _StatementResult__isset() : resultSet(false), updateCount(false), batchUpdateCounts(false), procedureOutParams(false), generatedKeys(false), newDefaultSchema(false), warnings(false), preparedResult(false) {}
  bool resultSet :1;
  bool updateCount :1;
  bool batchUpdateCounts :1;
  bool procedureOutParams :1;
  bool generatedKeys :1;
  bool newDefaultSchema :1;
  bool warnings :1;
  bool preparedResult :1;
} _StatementResult__isset;

class StatementResult {
 public:

  StatementResult(const StatementResult&);
  StatementResult(StatementResult&&) noexcept;
  StatementResult& operator=(const StatementResult&);
  StatementResult& operator=(StatementResult&&) noexcept;
  StatementResult() : updateCount(0), newDefaultSchema() {
  }

  virtual ~StatementResult() noexcept;
  RowSet resultSet;
  int32_t updateCount;
  std::vector<int32_t>  batchUpdateCounts;
  std::map<int32_t, ColumnValue>  procedureOutParams;
  RowSet generatedKeys;
  std::string newDefaultSchema;
  SnappyExceptionData warnings;
  PrepareResult preparedResult;

  _StatementResult__isset __isset;

  void __set_resultSet(const RowSet& val);

  void __set_updateCount(const int32_t val);

  void __set_batchUpdateCounts(const std::vector<int32_t> & val);

  void __set_procedureOutParams(const std::map<int32_t, ColumnValue> & val);

  void __set_generatedKeys(const RowSet& val);

  void __set_newDefaultSchema(const std::string& val);

  void __set_warnings(const SnappyExceptionData& val);

  void __set_preparedResult(const PrepareResult& val);

  bool operator == (const StatementResult & rhs) const
  {
    if (__isset.resultSet != rhs.__isset.resultSet)
      return false;
    else if (__isset.resultSet && !(resultSet == rhs.resultSet))
      return false;
    if (__isset.updateCount != rhs.__isset.updateCount)
      return false;
    else if (__isset.updateCount && !(updateCount == rhs.updateCount))
      return false;
    if (__isset.batchUpdateCounts != rhs.__isset.batchUpdateCounts)
      return false;
    else if (__isset.batchUpdateCounts && !(batchUpdateCounts == rhs.batchUpdateCounts))
      return false;
    if (__isset.procedureOutParams != rhs.__isset.procedureOutParams)
      return false;
    else if (__isset.procedureOutParams && !(procedureOutParams == rhs.procedureOutParams))
      return false;
    if (__isset.generatedKeys != rhs.__isset.generatedKeys)
      return false;
    else if (__isset.generatedKeys && !(generatedKeys == rhs.generatedKeys))
      return false;
    if (__isset.newDefaultSchema != rhs.__isset.newDefaultSchema)
      return false;
    else if (__isset.newDefaultSchema && !(newDefaultSchema == rhs.newDefaultSchema))
      return false;
    if (__isset.warnings != rhs.__isset.warnings)
      return false;
    else if (__isset.warnings && !(warnings == rhs.warnings))
      return false;
    if (__isset.preparedResult != rhs.__isset.preparedResult)
      return false;
    else if (__isset.preparedResult && !(preparedResult == rhs.preparedResult))
      return false;
    return true;
  }
  bool operator != (const StatementResult &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const StatementResult & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

  virtual void printTo(std::ostream& out) const;
};

void swap(StatementResult &a, StatementResult &b);

inline std::ostream& operator<<(std::ostream& out, const StatementResult& obj)
{
  obj.printTo(out);
  return out;
}

}}} // namespace

#endif
