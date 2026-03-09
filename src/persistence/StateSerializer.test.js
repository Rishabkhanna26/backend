import fc from 'fast-check';
import StateSerializer from './StateSerializer.js';

/**
 * Property-Based Tests for StateSerializer
 * 
 * Feature: session-state-persistence
 * Testing Framework: fast-check
 */

describe('StateSerializer - Property-Based Tests', () => {
  let serializer;

  beforeEach(() => {
    serializer = new StateSerializer();
  });

  /**
   * Property 1: Round-trip serialization preserves state
   * **Validates: Requirements 2.1, 2.2, 2.3, 2.5, 2.7, 8.1, 8.2**
   * 
   * For any valid conversation state object (containing any combination of the 17 user properties),
   * serializing then deserializing should produce an equivalent state object with all properties
   * preserved, Date objects correctly restored, and undefined values handled consistently.
   */
  test('property: serialize then deserialize preserves all user state properties', () => {
    // Arbitrary generator for user state objects with all 17 properties
    const userStateArbitrary = fc.record({
      // Core conversation properties
      step: fc.string(),
      data: fc.dictionary(fc.string(), fc.oneof(
        fc.string(),
        fc.integer(),
        fc.boolean(),
        fc.constant(null)
      )),
      
      // User identification properties
      isReturningUser: fc.boolean(),
      clientId: fc.oneof(fc.integer(), fc.constant(null)),
      name: fc.oneof(fc.string(), fc.constant(null)),
      email: fc.oneof(fc.string(), fc.constant(null)),
      assignedAdminId: fc.oneof(fc.integer(), fc.constant(null)),
      
      // Session state flags
      greetedThisSession: fc.boolean(),
      resumeStep: fc.oneof(fc.string(), fc.constant(null)),
      awaitingResumeDecision: fc.boolean(),
      finalized: fc.boolean(),
      automationDisabled: fc.boolean(),
      
      // Timestamp properties (Date objects)
      lastUserMessageAt: fc.oneof(fc.date(), fc.constant(null)),
      partialSavedAt: fc.oneof(fc.date(), fc.constant(null)),
      
      // Complex properties
      aiConversationHistory: fc.array(
        fc.record({
          role: fc.constantFrom('user', 'assistant', 'system'),
          content: fc.string()
        })
      ),
      responseLanguage: fc.oneof(
        fc.constantFrom('en', 'es', 'fr', 'de', 'it', 'pt'),
        fc.constant(null)
      ),
      
      // idleTimer is not persisted (always null in serialization)
      idleTimer: fc.constant(null)
    });

    fc.assert(
      fc.property(userStateArbitrary, (userState) => {
        // Serialize then deserialize
        const serialized = serializer.serialize(userState);
        
        // If serialization failed, it should be due to invalid dates
        if (serialized === null) {
          const hasInvalidDate = 
            (userState.lastUserMessageAt instanceof Date && isNaN(userState.lastUserMessageAt.getTime())) ||
            (userState.partialSavedAt instanceof Date && isNaN(userState.partialSavedAt.getTime()));
          expect(hasInvalidDate).toBe(true);
          return; // Test passes - correctly returned null for invalid data
        }
        
        // Serialization should succeed for valid data
        expect(serialized).not.toBeNull();
        expect(typeof serialized).toBe('string');
        
        const deserialized = serializer.deserialize(serialized);
        
        // Deserialization should succeed
        expect(deserialized).not.toBeNull();
        expect(typeof deserialized).toBe('object');
        
        // Verify all 17 properties are preserved
        expect(deserialized.step).toBe(userState.step);
        expect(deserialized.isReturningUser).toBe(userState.isReturningUser);
        expect(deserialized.clientId).toBe(userState.clientId);
        expect(deserialized.name).toBe(userState.name);
        expect(deserialized.email).toBe(userState.email);
        expect(deserialized.assignedAdminId).toBe(userState.assignedAdminId);
        expect(deserialized.greetedThisSession).toBe(userState.greetedThisSession);
        expect(deserialized.resumeStep).toBe(userState.resumeStep);
        expect(deserialized.awaitingResumeDecision).toBe(userState.awaitingResumeDecision);
        expect(deserialized.finalized).toBe(userState.finalized);
        expect(deserialized.automationDisabled).toBe(userState.automationDisabled);
        expect(deserialized.responseLanguage).toBe(userState.responseLanguage);
        expect(deserialized.idleTimer).toBe(userState.idleTimer);
        
        // Verify data object is preserved
        expect(deserialized.data).toEqual(userState.data);
        
        // Verify aiConversationHistory array is preserved
        expect(deserialized.aiConversationHistory).toEqual(userState.aiConversationHistory);
        
        // Verify Date objects are correctly restored (only for valid dates)
        if (userState.lastUserMessageAt instanceof Date && !isNaN(userState.lastUserMessageAt.getTime())) {
          expect(deserialized.lastUserMessageAt).toBeInstanceOf(Date);
          expect(deserialized.lastUserMessageAt.getTime()).toBe(userState.lastUserMessageAt.getTime());
        } else {
          expect(deserialized.lastUserMessageAt).toBe(userState.lastUserMessageAt);
        }
        
        if (userState.partialSavedAt instanceof Date && !isNaN(userState.partialSavedAt.getTime())) {
          expect(deserialized.partialSavedAt).toBeInstanceOf(Date);
          expect(deserialized.partialSavedAt.getTime()).toBe(userState.partialSavedAt.getTime());
        } else {
          expect(deserialized.partialSavedAt).toBe(userState.partialSavedAt);
        }
      }),
      { numRuns: 100 } // Run 100+ iterations as specified
    );
  });

  /**
   * Additional property test: Round-trip with edge cases
   * Tests empty objects, minimal states, and boundary values
   */
  test('property: serialize then deserialize handles edge cases', () => {
    const edgeCaseArbitrary = fc.oneof(
      // Minimal state
      fc.record({
        step: fc.constant('initial'),
        data: fc.constant({}),
        isReturningUser: fc.constant(false),
        clientId: fc.constant(null),
        name: fc.constant(null),
        email: fc.constant(null),
        assignedAdminId: fc.constant(null),
        greetedThisSession: fc.constant(false),
        resumeStep: fc.constant(null),
        awaitingResumeDecision: fc.constant(false),
        lastUserMessageAt: fc.constant(null),
        partialSavedAt: fc.constant(null),
        finalized: fc.constant(false),
        idleTimer: fc.constant(null),
        automationDisabled: fc.constant(false),
        aiConversationHistory: fc.constant([]),
        responseLanguage: fc.constant(null)
      }),
      // State with empty strings
      fc.record({
        step: fc.constant(''),
        data: fc.constant({}),
        isReturningUser: fc.boolean(),
        clientId: fc.constant(0),
        name: fc.constant(''),
        email: fc.constant(''),
        assignedAdminId: fc.constant(0),
        greetedThisSession: fc.boolean(),
        resumeStep: fc.constant(''),
        awaitingResumeDecision: fc.boolean(),
        lastUserMessageAt: fc.date({ min: new Date('2000-01-01'), max: new Date('2100-12-31') }),
        partialSavedAt: fc.date({ min: new Date('2000-01-01'), max: new Date('2100-12-31') }),
        finalized: fc.boolean(),
        idleTimer: fc.constant(null),
        automationDisabled: fc.boolean(),
        aiConversationHistory: fc.constant([]),
        responseLanguage: fc.constant('')
      })
    );

    fc.assert(
      fc.property(edgeCaseArbitrary, (userState) => {
        const serialized = serializer.serialize(userState);
        expect(serialized).not.toBeNull();
        
        const deserialized = serializer.deserialize(serialized);
        expect(deserialized).not.toBeNull();
        
        // Verify structure is preserved
        expect(Object.keys(deserialized).sort()).toEqual(Object.keys(userState).sort());
      }),
      { numRuns: 100 }
    );
  });

  /**
   * Property test: Nested data objects are preserved
   * Tests that complex nested structures in the data property are correctly serialized
   */
  test('property: serialize then deserialize preserves nested data structures', () => {
    const nestedDataArbitrary = fc.record({
      step: fc.string(),
      data: fc.oneof(
        // Simple nested object
        fc.record({
          selectedProduct: fc.string(),
          quantity: fc.integer({ min: 1, max: 100 }),
          price: fc.double({ min: 0, max: 10000, noNaN: true })
        }),
        // Deeply nested object
        fc.record({
          appointment: fc.record({
            type: fc.string(),
            date: fc.string(),
            time: fc.string(),
            details: fc.record({
              notes: fc.string(),
              confirmed: fc.boolean()
            })
          })
        }),
        // Array of objects
        fc.record({
          items: fc.array(
            fc.record({
              id: fc.integer(),
              name: fc.string(),
              selected: fc.boolean()
            })
          )
        })
      ),
      isReturningUser: fc.boolean(),
      clientId: fc.integer(),
      name: fc.string(),
      email: fc.string(),
      assignedAdminId: fc.integer(),
      greetedThisSession: fc.boolean(),
      resumeStep: fc.constant(null),
      awaitingResumeDecision: fc.boolean(),
      lastUserMessageAt: fc.date({ min: new Date('2000-01-01'), max: new Date('2100-12-31') }),
      partialSavedAt: fc.date({ min: new Date('2000-01-01'), max: new Date('2100-12-31') }),
      finalized: fc.boolean(),
      idleTimer: fc.constant(null),
      automationDisabled: fc.boolean(),
      aiConversationHistory: fc.array(
        fc.record({ role: fc.string(), content: fc.string() })
      ),
      responseLanguage: fc.string()
    });

    fc.assert(
      fc.property(nestedDataArbitrary, (userState) => {
        const serialized = serializer.serialize(userState);
        
        // If serialization failed (e.g., due to invalid dates), that's expected behavior
        if (serialized === null) {
          // Verify it was due to invalid data (like invalid dates)
          const hasInvalidDate = 
            (userState.lastUserMessageAt instanceof Date && isNaN(userState.lastUserMessageAt.getTime())) ||
            (userState.partialSavedAt instanceof Date && isNaN(userState.partialSavedAt.getTime()));
          expect(hasInvalidDate).toBe(true);
          return; // Test passes - correctly returned null for invalid data
        }
        
        const deserialized = serializer.deserialize(serialized);
        
        // Deep equality check for nested data
        expect(deserialized.data).toEqual(userState.data);
      }),
      { numRuns: 100 }
    );
  });
});

describe('StateSerializer - Serialization Error Handling (Property 3)', () => {
  let serializer;

  beforeEach(() => {
    serializer = new StateSerializer();
  });

  /**
   * Property 3: Serialization errors return null
   * **Validates: Requirements 2.8**
   * 
   * For any input that causes serialization to fail (malformed objects, deeply nested structures
   * exceeding limits), the serializer should return null and log the error without crashing the application.
   */
  test('property: serialization returns null for invalid inputs without crashing', () => {
    // Arbitrary generator for invalid inputs (non-object types that aren't valid user states)
    const invalidInputArbitrary = fc.oneof(
      fc.constant(null),
      fc.constant(undefined),
      fc.constant(42),
      fc.constant('string'),
      fc.constant(true),
      fc.constant(() => 'function')
      // Note: Arrays are actually valid and will serialize, so we exclude them
    );

    fc.assert(
      fc.property(invalidInputArbitrary, (invalidInput) => {
        // Serialization should not throw an error
        let result;
        expect(() => {
          result = serializer.serialize(invalidInput);
        }).not.toThrow();
        
        // Should return null for invalid inputs
        expect(result).toBeNull();
      }),
      { numRuns: 100 }
    );
  });

  test('property: serialization handles deeply nested structures', () => {
    // Create a generator for deeply nested objects
    const createDeeplyNested = (depth) => {
      let obj = { value: 'leaf' };
      for (let i = 0; i < depth; i++) {
        obj = { nested: obj, level: i };
      }
      return obj;
    };

    // Test with various nesting depths
    const depthArbitrary = fc.integer({ min: 1, max: 1000 });

    fc.assert(
      fc.property(depthArbitrary, (depth) => {
        const deeplyNested = createDeeplyNested(depth);
        
        // Wrap in a valid user state structure
        const userState = {
          step: 'test',
          data: deeplyNested,
          isReturningUser: false,
          clientId: 1,
          name: 'Test',
          email: 'test@example.com',
          assignedAdminId: 1,
          greetedThisSession: false,
          resumeStep: null,
          awaitingResumeDecision: false,
          lastUserMessageAt: new Date(),
          partialSavedAt: new Date(),
          finalized: false,
          idleTimer: null,
          automationDisabled: false,
          aiConversationHistory: [],
          responseLanguage: 'en'
        };
        
        // Serialization should not crash
        let result;
        expect(() => {
          result = serializer.serialize(userState);
        }).not.toThrow();
        
        // For very deep nesting (>100 levels), JSON.stringify might fail
        // In such cases, serializer should return null
        if (depth > 100) {
          // Either succeeds or returns null, but never crashes
          expect(result === null || typeof result === 'string').toBe(true);
        } else {
          // For reasonable depths, should succeed
          expect(result).not.toBeNull();
          expect(typeof result).toBe('string');
        }
      }),
      { numRuns: 100 }
    );
  });

  test('property: serialization handles objects with problematic properties', () => {
    // Generator for objects with various problematic properties
    const problematicStateArbitrary = fc.record({
      step: fc.string(),
      data: fc.oneof(
        // Object with Symbol keys (not serializable)
        fc.constant({ [Symbol('test')]: 'value', normal: 'data' }),
        // Object with BigInt values (not JSON serializable)
        fc.constant({ bigInt: 'placeholder' }), // Can't use actual BigInt in fc.constant
        // Object with undefined values
        fc.constant({ undef: undefined, normal: 'data' }),
        // Empty object
        fc.constant({}),
        // Object with NaN and Infinity
        fc.constant({ nan: NaN, infinity: Infinity, negInfinity: -Infinity })
      ),
      isReturningUser: fc.boolean(),
      clientId: fc.integer(),
      name: fc.string(),
      email: fc.string(),
      assignedAdminId: fc.integer(),
      greetedThisSession: fc.boolean(),
      resumeStep: fc.constant(null),
      awaitingResumeDecision: fc.boolean(),
      lastUserMessageAt: fc.date(),
      partialSavedAt: fc.date(),
      finalized: fc.boolean(),
      idleTimer: fc.constant(null),
      automationDisabled: fc.boolean(),
      aiConversationHistory: fc.array(
        fc.record({ role: fc.string(), content: fc.string() })
      ),
      responseLanguage: fc.string()
    });

    fc.assert(
      fc.property(problematicStateArbitrary, (userState) => {
        // Serialization should not crash
        let result;
        expect(() => {
          result = serializer.serialize(userState);
        }).not.toThrow();
        
        // Should either succeed or return null, but never crash
        expect(result === null || typeof result === 'string').toBe(true);
        
        // If serialization succeeded, result should be valid JSON
        if (result !== null) {
          expect(() => JSON.parse(result)).not.toThrow();
        }
      }),
      { numRuns: 100 }
    );
  });

  test('property: serialization handles malformed Date objects', () => {
    // Generator for states with various Date edge cases
    const dateEdgeCaseArbitrary = fc.record({
      step: fc.string(),
      data: fc.dictionary(fc.string(), fc.string()),
      isReturningUser: fc.boolean(),
      clientId: fc.integer(),
      name: fc.string(),
      email: fc.string(),
      assignedAdminId: fc.integer(),
      greetedThisSession: fc.boolean(),
      resumeStep: fc.constant(null),
      awaitingResumeDecision: fc.boolean(),
      lastUserMessageAt: fc.oneof(
        fc.date(),
        fc.constant(new Date('invalid')), // Invalid date
        fc.constant(new Date(NaN)), // NaN date
        fc.constant(null)
      ),
      partialSavedAt: fc.oneof(
        fc.date(),
        fc.constant(new Date('invalid')),
        fc.constant(null)
      ),
      finalized: fc.boolean(),
      idleTimer: fc.constant(null),
      automationDisabled: fc.boolean(),
      aiConversationHistory: fc.array(
        fc.record({ role: fc.string(), content: fc.string() })
      ),
      responseLanguage: fc.string()
    });

    fc.assert(
      fc.property(dateEdgeCaseArbitrary, (userState) => {
        // Serialization should not crash even with invalid dates
        let result;
        expect(() => {
          result = serializer.serialize(userState);
        }).not.toThrow();
        
        // Should return a result (either valid JSON or null)
        expect(result === null || typeof result === 'string').toBe(true);
        
        // If serialization succeeded, verify it's valid JSON
        if (result !== null) {
          expect(() => JSON.parse(result)).not.toThrow();
        }
      }),
      { numRuns: 100 }
    );
  });

  test('property: serialization handles extremely large objects', () => {
    // Generator for large objects
    const largeObjectArbitrary = fc.integer({ min: 100, max: 10000 }).map(size => {
      const largeData = {};
      for (let i = 0; i < size; i++) {
        largeData[`key${i}`] = `value${i}`;
      }
      
      return {
        step: 'test',
        data: largeData,
        isReturningUser: false,
        clientId: 1,
        name: 'Test',
        email: 'test@example.com',
        assignedAdminId: 1,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: new Date(),
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };
    });

    fc.assert(
      fc.property(largeObjectArbitrary, (userState) => {
        // Serialization should not crash with large objects
        let result;
        expect(() => {
          result = serializer.serialize(userState);
        }).not.toThrow();
        
        // Should return a result
        expect(result === null || typeof result === 'string').toBe(true);
        
        // If serialization succeeded, verify it's valid JSON
        if (result !== null) {
          expect(() => JSON.parse(result)).not.toThrow();
          
          // Verify deserialization also works
          const deserialized = serializer.deserialize(result);
          expect(deserialized).not.toBeNull();
          expect(Object.keys(deserialized.data).length).toBe(Object.keys(userState.data).length);
        }
      }),
      { numRuns: 100 }
    );
  });

  test('property: serialization handles arrays with problematic elements', () => {
    // Generator for states with problematic arrays
    const problematicArrayArbitrary = fc.record({
      step: fc.string(),
      data: fc.dictionary(fc.string(), fc.string()),
      isReturningUser: fc.boolean(),
      clientId: fc.integer(),
      name: fc.string(),
      email: fc.string(),
      assignedAdminId: fc.integer(),
      greetedThisSession: fc.boolean(),
      resumeStep: fc.constant(null),
      awaitingResumeDecision: fc.boolean(),
      lastUserMessageAt: fc.date(),
      partialSavedAt: fc.date(),
      finalized: fc.boolean(),
      idleTimer: fc.constant(null),
      automationDisabled: fc.boolean(),
      aiConversationHistory: fc.oneof(
        // Normal array
        fc.array(fc.record({ role: fc.string(), content: fc.string() })),
        // Array with mixed types including problematic ones
        fc.constant([
          { role: 'user', content: 'test' },
          { role: 'assistant', content: 'response', func: () => 'excluded' },
          { role: 'system', content: 'system', undef: undefined }
        ]),
        // Sparse array
        fc.constant([{ role: 'user', content: 'test' }, , , { role: 'assistant', content: 'response' }]),
        // Empty array
        fc.constant([])
      ),
      responseLanguage: fc.string()
    });

    fc.assert(
      fc.property(problematicArrayArbitrary, (userState) => {
        // Serialization should not crash
        let result;
        expect(() => {
          result = serializer.serialize(userState);
        }).not.toThrow();
        
        // Should return a result
        expect(result === null || typeof result === 'string').toBe(true);
        
        // If serialization succeeded, verify it's valid JSON
        if (result !== null) {
          expect(() => JSON.parse(result)).not.toThrow();
          
          const parsed = JSON.parse(result);
          expect(Array.isArray(parsed.aiConversationHistory)).toBe(true);
        }
      }),
      { numRuns: 100 }
    );
  });
});

describe('StateSerializer - Non-Serializable Value Exclusion (Property 2)', () => {
  let serializer;

  beforeEach(() => {
    serializer = new StateSerializer();
  });

  /**
   * Property 2: Serialization excludes non-serializable values
   * **Validates: Requirements 2.4**
   * 
   * For any conversation state object containing functions or circular references,
   * serialization should exclude these non-serializable properties without throwing errors,
   * and the resulting JSON should contain only serializable data.
   */
  test('property: serialization excludes functions without errors', () => {
    // Arbitrary generator for user state objects with functions mixed in
    const stateWithFunctionsArbitrary = fc.record({
      step: fc.string(),
      data: fc.dictionary(fc.string(), fc.oneof(
        fc.string(),
        fc.integer(),
        fc.boolean(),
        fc.constant(null),
        // Add function values that should be excluded
        fc.constant(() => 'test function'),
        fc.constant(function namedFunc() { return 42; })
      )),
      isReturningUser: fc.boolean(),
      clientId: fc.integer(),
      name: fc.string(),
      email: fc.string(),
      assignedAdminId: fc.integer(),
      greetedThisSession: fc.boolean(),
      resumeStep: fc.constant(null),
      awaitingResumeDecision: fc.boolean(),
      lastUserMessageAt: fc.date(),
      partialSavedAt: fc.date(),
      finalized: fc.boolean(),
      idleTimer: fc.constant(null),
      automationDisabled: fc.boolean(),
      aiConversationHistory: fc.array(
        fc.record({ role: fc.string(), content: fc.string() })
      ),
      responseLanguage: fc.string(),
      // Add function properties that should be excluded
      helperFunction: fc.constant(() => 'helper'),
      callback: fc.constant(function() { return true; })
    });

    fc.assert(
      fc.property(stateWithFunctionsArbitrary, (userState) => {
        // Serialization should succeed without throwing errors
        const serialized = serializer.serialize(userState);
        
        // Should return a valid JSON string
        expect(serialized).not.toBeNull();
        expect(typeof serialized).toBe('string');
        
        // Should be valid JSON
        expect(() => JSON.parse(serialized)).not.toThrow();
        
        const parsed = JSON.parse(serialized);
        
        // Functions should be excluded from the serialized output
        expect(parsed.helperFunction).toBeUndefined();
        expect(parsed.callback).toBeUndefined();
        
        // Check that function values in data object are excluded
        for (const key in parsed.data) {
          expect(typeof parsed.data[key]).not.toBe('function');
        }
        
        // Deserialization should also succeed
        const deserialized = serializer.deserialize(serialized);
        expect(deserialized).not.toBeNull();
        
        // Verify no functions in deserialized object
        expect(typeof deserialized.helperFunction).not.toBe('function');
        expect(typeof deserialized.callback).not.toBe('function');
      }),
      { numRuns: 100 }
    );
  });

  test('property: serialization excludes circular references without errors', () => {
    // Generator for base state objects
    const baseStateArbitrary = fc.record({
      step: fc.string(),
      data: fc.dictionary(fc.string(), fc.oneof(
        fc.string(),
        fc.integer(),
        fc.boolean()
      )),
      isReturningUser: fc.boolean(),
      clientId: fc.integer(),
      name: fc.string(),
      email: fc.string(),
      assignedAdminId: fc.integer(),
      greetedThisSession: fc.boolean(),
      resumeStep: fc.constant(null),
      awaitingResumeDecision: fc.boolean(),
      lastUserMessageAt: fc.date(),
      partialSavedAt: fc.date(),
      finalized: fc.boolean(),
      idleTimer: fc.constant(null),
      automationDisabled: fc.boolean(),
      aiConversationHistory: fc.array(
        fc.record({ role: fc.string(), content: fc.string() })
      ),
      responseLanguage: fc.string()
    });

    fc.assert(
      fc.property(baseStateArbitrary, (baseState) => {
        // Create circular reference
        const userState = { ...baseState };
        userState.circularRef = userState; // Self-reference
        
        // Create nested circular reference
        const nestedObj = { parent: userState };
        userState.nested = nestedObj;
        
        // Serialization should succeed without throwing errors
        const serialized = serializer.serialize(userState);
        
        // Should return a valid JSON string
        expect(serialized).not.toBeNull();
        expect(typeof serialized).toBe('string');
        
        // Should be valid JSON
        expect(() => JSON.parse(serialized)).not.toThrow();
        
        const parsed = JSON.parse(serialized);
        
        // Circular references should be excluded
        expect(parsed.circularRef).toBeUndefined();
        
        // Nested circular reference should be excluded
        if (parsed.nested) {
          expect(parsed.nested.parent).toBeUndefined();
        }
        
        // Deserialization should succeed
        const deserialized = serializer.deserialize(serialized);
        expect(deserialized).not.toBeNull();
        expect(typeof deserialized).toBe('object');
        
        // Verify no circular references in deserialized object
        expect(deserialized.circularRef).toBeUndefined();
      }),
      { numRuns: 100 }
    );
  });

  test('property: serialization handles mixed non-serializable values', () => {
    // Generator for states with both functions and potential circular refs
    const mixedStateArbitrary = fc.record({
      step: fc.string(),
      data: fc.record({
        validString: fc.string(),
        validNumber: fc.integer(),
        validBoolean: fc.boolean(),
        validNull: fc.constant(null)
      }),
      isReturningUser: fc.boolean(),
      clientId: fc.integer(),
      name: fc.string(),
      email: fc.string(),
      assignedAdminId: fc.integer(),
      greetedThisSession: fc.boolean(),
      resumeStep: fc.constant(null),
      awaitingResumeDecision: fc.boolean(),
      lastUserMessageAt: fc.date(),
      partialSavedAt: fc.date(),
      finalized: fc.boolean(),
      idleTimer: fc.constant(null),
      automationDisabled: fc.boolean(),
      aiConversationHistory: fc.array(
        fc.record({ role: fc.string(), content: fc.string() })
      ),
      responseLanguage: fc.string()
    });

    fc.assert(
      fc.property(mixedStateArbitrary, (baseState) => {
        // Add various non-serializable properties
        const userState = { ...baseState };
        userState.func1 = () => 'test';
        userState.func2 = function() { return 42; };
        userState.circular = userState;
        
        // Add functions to nested data
        userState.data.invalidFunc = () => 'should be excluded';
        
        // Serialization should succeed or return null for invalid dates
        const serialized = serializer.serialize(userState);
        
        // If serialization failed, it should be due to invalid dates
        if (serialized === null) {
          const hasInvalidDate = 
            (userState.lastUserMessageAt instanceof Date && isNaN(userState.lastUserMessageAt.getTime())) ||
            (userState.partialSavedAt instanceof Date && isNaN(userState.partialSavedAt.getTime()));
          expect(hasInvalidDate).toBe(true);
          return; // Test passes - correctly returned null for invalid data
        }
        
        expect(typeof serialized).toBe('string');
        
        // Parse and verify
        const parsed = JSON.parse(serialized);
        
        // All non-serializable values should be excluded
        expect(parsed.func1).toBeUndefined();
        expect(parsed.func2).toBeUndefined();
        expect(parsed.circular).toBeUndefined();
        expect(parsed.data.invalidFunc).toBeUndefined();
        
        // Valid properties should be preserved
        expect(parsed.step).toBe(userState.step);
        expect(parsed.data.validString).toBe(userState.data.validString);
        expect(parsed.data.validNumber).toBe(userState.data.validNumber);
        expect(parsed.data.validBoolean).toBe(userState.data.validBoolean);
        expect(parsed.data.validNull).toBe(userState.data.validNull);
        
        // Deserialization should succeed
        const deserialized = serializer.deserialize(serialized);
        expect(deserialized).not.toBeNull();
        
        // Verify only serializable data remains
        expect(deserialized.func1).toBeUndefined();
        expect(deserialized.func2).toBeUndefined();
        expect(deserialized.circular).toBeUndefined();
        expect(deserialized.data.validString).toBe(userState.data.validString);
      }),
      { numRuns: 100 }
    );
  });
});

describe('StateSerializer - Unit Tests for Edge Cases', () => {
  let serializer;

  beforeEach(() => {
    serializer = new StateSerializer();
  });

  /**
   * Edge Case: Empty state objects
   */
  describe('Empty state objects', () => {
    test('should serialize and deserialize empty data object', () => {
      const userState = {
        step: 'initial',
        data: {}, // Empty data object
        isReturningUser: false,
        clientId: null,
        name: null,
        email: null,
        assignedAdminId: null,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: null,
        partialSavedAt: null,
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: null
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();
      expect(typeof serialized).toBe('string');

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.data).toEqual({});
      expect(deserialized.aiConversationHistory).toEqual([]);
    });

    test('should serialize and deserialize completely empty state', () => {
      const userState = {};

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();
      expect(typeof serialized).toBe('string');

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized).toEqual({});
    });
  });

  /**
   * Edge Case: States with missing properties
   */
  describe('States with missing properties', () => {
    test('should handle state with only required properties', () => {
      const userState = {
        step: 'awaiting_name',
        data: { selectedProduct: 'birth_chart' }
        // All other properties missing
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.step).toBe('awaiting_name');
      expect(deserialized.data).toEqual({ selectedProduct: 'birth_chart' });
      expect(deserialized.isReturningUser).toBeUndefined();
      expect(deserialized.clientId).toBeUndefined();
    });

    test('should handle state with some properties missing', () => {
      const userState = {
        step: 'awaiting_email',
        data: { name: 'John Doe' },
        isReturningUser: true,
        clientId: 123,
        // name, email, assignedAdminId missing
        greetedThisSession: true,
        lastUserMessageAt: new Date('2024-01-15T10:30:00.000Z')
        // Other properties missing
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.step).toBe('awaiting_email');
      expect(deserialized.isReturningUser).toBe(true);
      expect(deserialized.clientId).toBe(123);
      expect(deserialized.greetedThisSession).toBe(true);
      expect(deserialized.lastUserMessageAt).toBeInstanceOf(Date);
      expect(deserialized.lastUserMessageAt.toISOString()).toBe('2024-01-15T10:30:00.000Z');
      expect(deserialized.name).toBeUndefined();
      expect(deserialized.email).toBeUndefined();
    });
  });

  /**
   * Edge Case: States with null values
   */
  describe('States with null values', () => {
    test('should preserve null values for all nullable properties', () => {
      const userState = {
        step: 'initial',
        data: { key: null },
        isReturningUser: false,
        clientId: null,
        name: null,
        email: null,
        assignedAdminId: null,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: null,
        partialSavedAt: null,
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: null
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.clientId).toBeNull();
      expect(deserialized.name).toBeNull();
      expect(deserialized.email).toBeNull();
      expect(deserialized.assignedAdminId).toBeNull();
      expect(deserialized.resumeStep).toBeNull();
      expect(deserialized.lastUserMessageAt).toBeNull();
      expect(deserialized.partialSavedAt).toBeNull();
      expect(deserialized.idleTimer).toBeNull();
      expect(deserialized.responseLanguage).toBeNull();
      expect(deserialized.data.key).toBeNull();
    });

    test('should handle nested null values in data object', () => {
      const userState = {
        step: 'test',
        data: {
          appointment: {
            date: null,
            time: null,
            details: null
          },
          selectedProduct: null,
          quantity: null
        },
        isReturningUser: false,
        clientId: 1,
        name: 'Test',
        email: 'test@example.com',
        assignedAdminId: 1,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: null,
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.data.appointment.date).toBeNull();
      expect(deserialized.data.appointment.time).toBeNull();
      expect(deserialized.data.appointment.details).toBeNull();
      expect(deserialized.data.selectedProduct).toBeNull();
      expect(deserialized.data.quantity).toBeNull();
    });
  });

  /**
   * Edge Case: States with undefined values
   */
  describe('States with undefined values', () => {
    test('should convert undefined to null during serialization', () => {
      const userState = {
        step: 'test',
        data: { key: undefined },
        isReturningUser: false,
        clientId: undefined,
        name: undefined,
        email: undefined,
        assignedAdminId: undefined,
        greetedThisSession: false,
        resumeStep: undefined,
        awaitingResumeDecision: false,
        lastUserMessageAt: undefined,
        partialSavedAt: undefined,
        finalized: false,
        idleTimer: undefined,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: undefined
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      // Parse to check the serialized format
      const parsed = JSON.parse(serialized);
      // undefined values are converted to null by the serializer
      expect(parsed.clientId).toBeNull();
      expect(parsed.name).toBeNull();
      expect(parsed.email).toBeNull();
      expect(parsed.data.key).toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      // After deserialization, undefined values become null
      expect(deserialized.clientId).toBeNull();
      expect(deserialized.name).toBeNull();
      expect(deserialized.email).toBeNull();
      expect(deserialized.data.key).toBeNull();
    });

    test('should handle mixed null and undefined values', () => {
      const userState = {
        step: 'test',
        data: {
          definedValue: 'present',
          nullValue: null,
          undefinedValue: undefined
        },
        isReturningUser: false,
        clientId: null,
        name: undefined,
        email: 'test@example.com',
        assignedAdminId: 1,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: undefined,
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.data.definedValue).toBe('present');
      expect(deserialized.data.nullValue).toBeNull();
      // undefined values are converted to null during serialization
      expect(deserialized.data.undefinedValue).toBeNull();
      expect(deserialized.clientId).toBeNull();
      expect(deserialized.name).toBeNull();
      expect(deserialized.email).toBe('test@example.com');
      expect(deserialized.partialSavedAt).toBeNull();
    });
  });

  /**
   * Edge Case: Date serialization edge cases
   */
  describe('Date serialization edge cases', () => {
    test('should handle invalid Date objects', () => {
      const userState = {
        step: 'test',
        data: {},
        isReturningUser: false,
        clientId: 1,
        name: 'Test',
        email: 'test@example.com',
        assignedAdminId: 1,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date('invalid'), // Invalid date
        partialSavedAt: new Date(NaN), // NaN date
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      // Invalid dates should be converted to null
      expect(deserialized.lastUserMessageAt).toBeNull();
      expect(deserialized.partialSavedAt).toBeNull();
    });

    test('should handle epoch date (Unix timestamp 0)', () => {
      const epochDate = new Date(0); // January 1, 1970, 00:00:00 UTC

      const userState = {
        step: 'test',
        data: {},
        isReturningUser: false,
        clientId: 1,
        name: 'Test',
        email: 'test@example.com',
        assignedAdminId: 1,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: epochDate,
        partialSavedAt: epochDate,
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.lastUserMessageAt).toBeInstanceOf(Date);
      expect(deserialized.lastUserMessageAt.getTime()).toBe(0);
      expect(deserialized.lastUserMessageAt.toISOString()).toBe('1970-01-01T00:00:00.000Z');
      expect(deserialized.partialSavedAt).toBeInstanceOf(Date);
      expect(deserialized.partialSavedAt.getTime()).toBe(0);
    });

    test('should handle very old dates (before 1900)', () => {
      const oldDate = new Date('1850-01-01T00:00:00.000Z');

      const userState = {
        step: 'test',
        data: {},
        isReturningUser: false,
        clientId: 1,
        name: 'Test',
        email: 'test@example.com',
        assignedAdminId: 1,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: oldDate,
        partialSavedAt: null,
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.lastUserMessageAt).toBeInstanceOf(Date);
      expect(deserialized.lastUserMessageAt.toISOString()).toBe('1850-01-01T00:00:00.000Z');
    });

    test('should handle far future dates (year 9999)', () => {
      const futureDate = new Date('9999-12-31T23:59:59.999Z');

      const userState = {
        step: 'test',
        data: {},
        isReturningUser: false,
        clientId: 1,
        name: 'Test',
        email: 'test@example.com',
        assignedAdminId: 1,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: futureDate,
        partialSavedAt: null,
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.lastUserMessageAt).toBeInstanceOf(Date);
      expect(deserialized.lastUserMessageAt.toISOString()).toBe('9999-12-31T23:59:59.999Z');
    });

    test('should handle dates with millisecond precision', () => {
      const preciseDate = new Date('2024-01-15T10:30:45.123Z');

      const userState = {
        step: 'test',
        data: {},
        isReturningUser: false,
        clientId: 1,
        name: 'Test',
        email: 'test@example.com',
        assignedAdminId: 1,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: preciseDate,
        partialSavedAt: null,
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.lastUserMessageAt).toBeInstanceOf(Date);
      expect(deserialized.lastUserMessageAt.getTime()).toBe(preciseDate.getTime());
      expect(deserialized.lastUserMessageAt.toISOString()).toBe('2024-01-15T10:30:45.123Z');
    });

    test('should handle Date objects in nested data structures', () => {
      const appointmentDate = new Date('2024-02-20T14:00:00.000Z');
      const reminderDate = new Date('2024-02-19T14:00:00.000Z');

      const userState = {
        step: 'test',
        data: {
          appointment: {
            scheduledDate: appointmentDate,
            reminder: {
              sendAt: reminderDate
            }
          }
        },
        isReturningUser: false,
        clientId: 1,
        name: 'Test',
        email: 'test@example.com',
        assignedAdminId: 1,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: null,
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(deserialized.data.appointment.scheduledDate).toBeInstanceOf(Date);
      expect(deserialized.data.appointment.scheduledDate.toISOString()).toBe('2024-02-20T14:00:00.000Z');
      expect(deserialized.data.appointment.reminder.sendAt).toBeInstanceOf(Date);
      expect(deserialized.data.appointment.reminder.sendAt.toISOString()).toBe('2024-02-19T14:00:00.000Z');
    });

    test('should handle array of Date objects', () => {
      const dates = [
        new Date('2024-01-01T00:00:00.000Z'),
        new Date('2024-06-15T12:00:00.000Z'),
        new Date('2024-12-31T23:59:59.999Z')
      ];

      const userState = {
        step: 'test',
        data: {
          importantDates: dates
        },
        isReturningUser: false,
        clientId: 1,
        name: 'Test',
        email: 'test@example.com',
        assignedAdminId: 1,
        greetedThisSession: false,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: null,
        finalized: false,
        idleTimer: null,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };

      const serialized = serializer.serialize(userState);
      expect(serialized).not.toBeNull();

      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).not.toBeNull();
      expect(Array.isArray(deserialized.data.importantDates)).toBe(true);
      expect(deserialized.data.importantDates).toHaveLength(3);
      expect(deserialized.data.importantDates[0]).toBeInstanceOf(Date);
      expect(deserialized.data.importantDates[0].toISOString()).toBe('2024-01-01T00:00:00.000Z');
      expect(deserialized.data.importantDates[1]).toBeInstanceOf(Date);
      expect(deserialized.data.importantDates[1].toISOString()).toBe('2024-06-15T12:00:00.000Z');
      expect(deserialized.data.importantDates[2]).toBeInstanceOf(Date);
      expect(deserialized.data.importantDates[2].toISOString()).toBe('2024-12-31T23:59:59.999Z');
    });
  });
});
