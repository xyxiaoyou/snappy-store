/**
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */

#ifndef SNAPPYDATA_STRUCT_BUCKETOWNERS_H
#define SNAPPYDATA_STRUCT_BUCKETOWNERS_H


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
#include "snappydata_struct_StatementResult.h"

#include "snappydata_types.h"

namespace io { namespace snappydata { namespace thrift {

typedef struct _BucketOwners__isset {
  _BucketOwners__isset() : primary(false), secondaries(false) {}
  bool primary :1;
  bool secondaries :1;
} _BucketOwners__isset;

class BucketOwners {
 public:

  BucketOwners(const BucketOwners&);
  BucketOwners(BucketOwners&&) noexcept;
  BucketOwners& operator=(const BucketOwners&);
  BucketOwners& operator=(BucketOwners&&) noexcept;
  BucketOwners() : bucketId(0), primary() {
  }

  virtual ~BucketOwners() noexcept;
  int32_t bucketId;
  std::string primary;
  std::vector<std::string>  secondaries;

  _BucketOwners__isset __isset;

  void __set_bucketId(const int32_t val);

  void __set_primary(const std::string& val);

  void __set_secondaries(const std::vector<std::string> & val);

  bool operator == (const BucketOwners & rhs) const
  {
    if (!(bucketId == rhs.bucketId))
      return false;
    if (__isset.primary != rhs.__isset.primary)
      return false;
    else if (__isset.primary && !(primary == rhs.primary))
      return false;
    if (__isset.secondaries != rhs.__isset.secondaries)
      return false;
    else if (__isset.secondaries && !(secondaries == rhs.secondaries))
      return false;
    return true;
  }
  bool operator != (const BucketOwners &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const BucketOwners & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

  virtual void printTo(std::ostream& out) const;
};

void swap(BucketOwners &a, BucketOwners &b);

inline std::ostream& operator<<(std::ostream& out, const BucketOwners& obj)
{
  obj.printTo(out);
  return out;
}

}}} // namespace

#endif
