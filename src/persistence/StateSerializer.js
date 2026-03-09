import logger from '../../config/logger.js';

/**
 * StateSerializer handles conversion of user conversation state objects to/from JSON
 * for database storage. Implements special handling for Date objects, undefined values,
 * and non-serializable properties (functions, circular references).
 * 
 * Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.8
 */
class StateSerializer {
  /**
   * Serialize a user state object to JSON string
   * 
   * @param {Object} userState - The user conversation state object
   * @returns {string|null} JSON string or null on error
   */
  serialize(userState) {
    try {
      if (!userState || typeof userState !== 'object') {
        logger.warn('StateSerializer: Invalid userState provided for serialization', {
          type: typeof userState
        });
        return null;
      }

      // Track circular references
      const seen = new WeakSet();
      
      const serialized = this._serializeValue(userState, seen);
      return JSON.stringify(serialized);
    } catch (error) {
      logger.error('StateSerializer: Serialization failed', {
        error: error.message,
        stack: error.stack
      });
      return null;
    }
  }

  /**
   * Deserialize a JSON string back to user state object
   * 
   * @param {string} jsonString - The JSON string to deserialize
   * @returns {Object|null} User state object or null on error
   */
  deserialize(jsonString) {
    try {
      if (!jsonString || typeof jsonString !== 'string') {
        logger.warn('StateSerializer: Invalid JSON string provided for deserialization', {
          type: typeof jsonString
        });
        return null;
      }

      const parsed = JSON.parse(jsonString);
      return this._deserializeValue(parsed);
    } catch (error) {
      logger.error('StateSerializer: Deserialization failed', {
        error: error.message,
        stack: error.stack
      });
      return null;
    }
  }

  /**
   * Internal helper to serialize a value with type preservation
   * 
   * @param {*} value - The value to serialize
   * @param {WeakSet} seen - Set to track circular references
   * @returns {*} Serialized value
   * @private
   */
  _serializeValue(value, seen) {
    // Handle null and undefined
    if (value === undefined) {
      return null;
    }
    
    if (value === null) {
      return null;
    }

    // Handle primitives
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      return value;
    }

    // Exclude functions
    if (typeof value === 'function') {
      logger.warn('StateSerializer: Function excluded from serialization');
      return undefined; // Will be filtered out by JSON.stringify
    }

    // Handle Date objects
    if (value instanceof Date) {
      // Check for invalid dates
      if (isNaN(value.getTime())) {
        logger.warn('StateSerializer: Invalid Date object detected, converting to null');
        return null;
      }
      return {
        __type: 'Date',
        value: value.toISOString()
      };
    }

    // Handle Arrays
    if (Array.isArray(value)) {
      return value.map(item => this._serializeValue(item, seen));
    }

    // Handle Objects
    if (typeof value === 'object') {
      // Check for circular references
      if (seen.has(value)) {
        logger.warn('StateSerializer: Circular reference detected and excluded');
        return undefined; // Will be filtered out by JSON.stringify
      }
      
      seen.add(value);

      const result = {};
      for (const key in value) {
        if (Object.prototype.hasOwnProperty.call(value, key)) {
          const serializedValue = this._serializeValue(value[key], seen);
          // Only include if not undefined (filters out functions and circular refs)
          if (serializedValue !== undefined) {
            result[key] = serializedValue;
          }
        }
      }
      
      return result;
    }

    // Fallback for other types
    return value;
  }

  /**
   * Internal helper to deserialize a value with type restoration
   * 
   * @param {*} value - The value to deserialize
   * @returns {*} Deserialized value
   * @private
   */
  _deserializeValue(value) {
    // Handle null
    if (value === null) {
      return null;
    }

    // Handle primitives
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      return value;
    }

    // Handle Arrays
    if (Array.isArray(value)) {
      return value.map(item => this._deserializeValue(item));
    }

    // Handle Objects
    if (typeof value === 'object') {
      // Check for Date marker
      if (value.__type === 'Date' && value.value) {
        return new Date(value.value);
      }

      // Recursively deserialize object properties
      const result = {};
      for (const key in value) {
        if (Object.prototype.hasOwnProperty.call(value, key)) {
          result[key] = this._deserializeValue(value[key]);
        }
      }
      
      return result;
    }

    // Fallback
    return value;
  }
}

export default StateSerializer;
export { StateSerializer };
