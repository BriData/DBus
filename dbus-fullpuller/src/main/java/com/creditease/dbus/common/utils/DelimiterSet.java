/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creditease.dbus.common.utils;

/**
 * Encapsulates a set of delimiters used to encode a record.
 */
public class DelimiterSet implements Cloneable {

  public static final char NULL_CHAR = '\000';

  private char fieldDelim; // fields terminated by this.
  private char recordDelim; // records terminated by this.

  // If these next two fields are '\000', then they are ignored.
  private char enclosedBy;
  private char escapedBy;

  // If true, then the enclosed-by character is applied to every
  // field, not just ones containing embedded delimiters.

  private boolean encloseRequired;

  /**
   * Create the input variations for export instead of overloading them.
   */
  public static final String INPUT_FIELD_DELIM_KEY =
      "input.field.delim";
  public static final String INPUT_RECORD_DELIM_KEY =
      "input.record.delim";
  public static final String INPUT_ENCLOSED_BY_KEY =
      "input.enclosed.by";
  public static final String INPUT_ESCAPED_BY_KEY =
      "input.escaped.by";
  public static final String INPUT_ENCLOSE_REQUIRED_KEY =
      "input.enclose.required";
  /**
   * Create a delimiter set with the default delimiters
   * (comma for fields, newline for records).
   */
  public DelimiterSet() {
    this(',', '\n', NULL_CHAR, NULL_CHAR, false);
  }

  /**
   * Create a delimiter set with the specified delimiters.
   * @param field the fields-terminated-by delimiter
   * @param record the lines-terminated-by delimiter
   * @param enclose the enclosed-by character
   * @param escape the escaped-by character
   * @param isEncloseRequired If true, enclosed-by is applied to all
   * fields. If false, only applied to fields that embed delimiters.
   */
  public DelimiterSet(char field, char record, char enclose, char escape,
      boolean isEncloseRequired) {
    this.fieldDelim = field;
    this.recordDelim = record;
    this.enclosedBy = enclose;
    this.escapedBy = escape;
    this.encloseRequired = isEncloseRequired;
  }

  /**
   * Identical to clone() but does not throw spurious exceptions.
   * @return a new copy of this same set of delimiters.
   */
  public DelimiterSet copy() {
    try {
      return (DelimiterSet) clone();
    } catch (CloneNotSupportedException cnse) {
      // Should never happen for DelimiterSet.
      return null;
    }
  }

  /**
   * Sets the fields-terminated-by character.
   */
  public void setFieldsTerminatedBy(char f) {
    this.fieldDelim = f;
  }

  /**
   * @return the fields-terminated-by character.
   */
  public char getFieldsTerminatedBy() {
    return this.fieldDelim;
  }

  /**
   * Sets the end-of-record lines-terminated-by character.
   */
  public void setLinesTerminatedBy(char r) {
    this.recordDelim = r;
  }

  /**
   * @return the end-of-record (lines-terminated-by) character.
   */
  public char getLinesTerminatedBy() {
    return this.recordDelim;
  }

  /**
   * Sets the enclosed-by character.
   * @param e the enclosed-by character, or '\000' for no enclosing character.
   */
  public void setEnclosedBy(char e) {
    this.enclosedBy = e;
  }

  /**
   * @return the enclosed-by character, or '\000' for none.
   */
  public char getEnclosedBy() {
    return this.enclosedBy;
  }

  /**
   * Sets the escaped-by character.
   * @param e the escaped-by character, or '\000' for no escape character.
   */
  public void setEscapedBy(char e) {
    this.escapedBy = e;
  }

  /**
   * @return the escaped-by character, or '\000' for none.
   */
  public char getEscapedBy() {
    return this.escapedBy;
  }

  /**
   * Set whether the enclosed-by character must be applied to all fields,
   * or only fields with embedded delimiters.
   */
  public void setEncloseRequired(boolean required) {
    this.encloseRequired = required;
  }

  /**
   * @return true if the enclosed-by character must be applied to all fields,
   * or false if it's only used for fields with embedded delimiters.
   */
  public boolean isEncloseRequired() {
    return this.encloseRequired;
  }

  @Override
  /**
   * @return a string representation of the delimiters.
   */
  public String toString() {
    return "fields=" + this.fieldDelim
        + " records=" + this.recordDelim
        + " escape=" + this.escapedBy
        + " enclose=" + this.enclosedBy
        + " required=" + this.encloseRequired;
  }

  /**
   * Format this set of delimiters as a call to the constructor for
   * this object, that would generate identical delimiters.
   * @return a String that can be embedded in generated code that
   * provides this set of delimiters.
   */
  public String formatConstructor() {
    return "new DelimiterSet((char) " + (int) this.fieldDelim + ", "
        + "(char) " + (int) this.recordDelim + ", "
        + "(char) " + (int) this.enclosedBy + ", "
        + "(char) " + (int) this.escapedBy + ", "
        + this.encloseRequired + ")";
  }

  @Override
  /**
   * @return a hash code for this set of delimiters.
   */
  public int hashCode() {
    return (int) this.fieldDelim
        + (((int) this.recordDelim) << 4)
        + (((int) this.escapedBy) << 8)
        + (((int) this.enclosedBy) << 12)
        + (((int) this.recordDelim) << 16)
        + (this.encloseRequired ? 0xFEFE : 0x7070);
  }

  @Override
  /**
   * @return true if this delimiter set is the same as another set of
   * delimiters.
   */
  public boolean equals(Object other) {
    if (null == other) {
      return false;
    } else if (!other.getClass().equals(getClass())) {
      return false;
    }

    DelimiterSet set = (DelimiterSet) other;
    return this.fieldDelim == set.fieldDelim
        && this.recordDelim == set.recordDelim
        && this.escapedBy == set.escapedBy
        && this.enclosedBy == set.enclosedBy
        && this.encloseRequired == set.encloseRequired;
  }

  @Override
  /**
   * @return a new copy of this same set of delimiters.
   */
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
