/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package io.snappydata.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum CursorUpdateOperation implements org.apache.thrift.TEnum {
  UPDATE_OP(1),
  INSERT_OP(2),
  DELETE_OP(3);

  private final int value;

  private CursorUpdateOperation(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static CursorUpdateOperation findByValue(int value) { 
    switch (value) {
      case 1:
        return UPDATE_OP;
      case 2:
        return INSERT_OP;
      case 3:
        return DELETE_OP;
      default:
        return null;
    }
  }
}
