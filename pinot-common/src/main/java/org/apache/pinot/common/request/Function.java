/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.19.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.pinot.common.request;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.19.0)", date = "2025-03-03")
public class Function implements org.apache.thrift.TBase<Function, Function._Fields>, java.io.Serializable, Cloneable, Comparable<Function> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Function");

  private static final org.apache.thrift.protocol.TField OPERATOR_FIELD_DESC = new org.apache.thrift.protocol.TField("operator", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField OPERANDS_FIELD_DESC = new org.apache.thrift.protocol.TField("operands", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new FunctionStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new FunctionTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable java.lang.String operator; // required
  private @org.apache.thrift.annotation.Nullable java.util.List<Expression> operands; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    OPERATOR((short)1, "operator"),
    OPERANDS((short)2, "operands");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // OPERATOR
          return OPERATOR;
        case 2: // OPERANDS
          return OPERANDS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.OPERANDS};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.OPERATOR, new org.apache.thrift.meta_data.FieldMetaData("operator", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.OPERANDS, new org.apache.thrift.meta_data.FieldMetaData("operands", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Expression.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Function.class, metaDataMap);
  }

  public Function() {
  }

  public Function(
    java.lang.String operator)
  {
    this();
    this.operator = operator;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Function(Function other) {
    if (other.isSetOperator()) {
      this.operator = other.operator;
    }
    if (other.isSetOperands()) {
      java.util.List<Expression> __this__operands = new java.util.ArrayList<Expression>(other.operands.size());
      for (Expression other_element : other.operands) {
        __this__operands.add(new Expression(other_element));
      }
      this.operands = __this__operands;
    }
  }

  @Override
  public Function deepCopy() {
    return new Function(this);
  }

  @Override
  public void clear() {
    this.operator = null;
    this.operands = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getOperator() {
    return this.operator;
  }

  public void setOperator(@org.apache.thrift.annotation.Nullable java.lang.String operator) {
    this.operator = operator;
  }

  public void unsetOperator() {
    this.operator = null;
  }

  /** Returns true if field operator is set (has been assigned a value) and false otherwise */
  public boolean isSetOperator() {
    return this.operator != null;
  }

  public void setOperatorIsSet(boolean value) {
    if (!value) {
      this.operator = null;
    }
  }

  public int getOperandsSize() {
    return (this.operands == null) ? 0 : this.operands.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<Expression> getOperandsIterator() {
    return (this.operands == null) ? null : this.operands.iterator();
  }

  public void addToOperands(Expression elem) {
    if (this.operands == null) {
      this.operands = new java.util.ArrayList<Expression>();
    }
    this.operands.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<Expression> getOperands() {
    return this.operands;
  }

  public void setOperands(@org.apache.thrift.annotation.Nullable java.util.List<Expression> operands) {
    this.operands = operands;
  }

  public void unsetOperands() {
    this.operands = null;
  }

  /** Returns true if field operands is set (has been assigned a value) and false otherwise */
  public boolean isSetOperands() {
    return this.operands != null;
  }

  public void setOperandsIsSet(boolean value) {
    if (!value) {
      this.operands = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case OPERATOR:
      if (value == null) {
        unsetOperator();
      } else {
        setOperator((java.lang.String)value);
      }
      break;

    case OPERANDS:
      if (value == null) {
        unsetOperands();
      } else {
        setOperands((java.util.List<Expression>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case OPERATOR:
      return getOperator();

    case OPERANDS:
      return getOperands();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case OPERATOR:
      return isSetOperator();
    case OPERANDS:
      return isSetOperands();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof Function)
      return this.equals((Function)that);
    return false;
  }

  public boolean equals(Function that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_operator = true && this.isSetOperator();
    boolean that_present_operator = true && that.isSetOperator();
    if (this_present_operator || that_present_operator) {
      if (!(this_present_operator && that_present_operator))
        return false;
      if (!this.operator.equals(that.operator))
        return false;
    }

    boolean this_present_operands = true && this.isSetOperands();
    boolean that_present_operands = true && that.isSetOperands();
    if (this_present_operands || that_present_operands) {
      if (!(this_present_operands && that_present_operands))
        return false;
      if (!this.operands.equals(that.operands))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetOperator()) ? 131071 : 524287);
    if (isSetOperator())
      hashCode = hashCode * 8191 + operator.hashCode();

    hashCode = hashCode * 8191 + ((isSetOperands()) ? 131071 : 524287);
    if (isSetOperands())
      hashCode = hashCode * 8191 + operands.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(Function other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetOperator(), other.isSetOperator());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOperator()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.operator, other.operator);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetOperands(), other.isSetOperands());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOperands()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.operands, other.operands);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("Function(");
    boolean first = true;

    sb.append("operator:");
    if (this.operator == null) {
      sb.append("null");
    } else {
      sb.append(this.operator);
    }
    first = false;
    if (isSetOperands()) {
      if (!first) sb.append(", ");
      sb.append("operands:");
      if (this.operands == null) {
        sb.append("null");
      } else {
        sb.append(this.operands);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetOperator()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'operator' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class FunctionStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public FunctionStandardScheme getScheme() {
      return new FunctionStandardScheme();
    }
  }

  private static class FunctionStandardScheme extends org.apache.thrift.scheme.StandardScheme<Function> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, Function struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // OPERATOR
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.operator = iprot.readString();
              struct.setOperatorIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // OPERANDS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list84 = iprot.readListBegin();
                struct.operands = new java.util.ArrayList<Expression>(_list84.size);
                @org.apache.thrift.annotation.Nullable Expression _elem85;
                for (int _i86 = 0; _i86 < _list84.size; ++_i86)
                {
                  _elem85 = new Expression();
                  _elem85.read(iprot);
                  struct.operands.add(_elem85);
                }
                iprot.readListEnd();
              }
              struct.setOperandsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, Function struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.operator != null) {
        oprot.writeFieldBegin(OPERATOR_FIELD_DESC);
        oprot.writeString(struct.operator);
        oprot.writeFieldEnd();
      }
      if (struct.operands != null) {
        if (struct.isSetOperands()) {
          oprot.writeFieldBegin(OPERANDS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.operands.size()));
            for (Expression _iter87 : struct.operands)
            {
              _iter87.write(oprot);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class FunctionTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public FunctionTupleScheme getScheme() {
      return new FunctionTupleScheme();
    }
  }

  private static class FunctionTupleScheme extends org.apache.thrift.scheme.TupleScheme<Function> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Function struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeString(struct.operator);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetOperands()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetOperands()) {
        {
          oprot.writeI32(struct.operands.size());
          for (Expression _iter88 : struct.operands)
          {
            _iter88.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Function struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.operator = iprot.readString();
      struct.setOperatorIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list89 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRUCT);
          struct.operands = new java.util.ArrayList<Expression>(_list89.size);
          @org.apache.thrift.annotation.Nullable Expression _elem90;
          for (int _i91 = 0; _i91 < _list89.size; ++_i91)
          {
            _elem90 = new Expression();
            _elem90.read(iprot);
            struct.operands.add(_elem90);
          }
        }
        struct.setOperandsIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

