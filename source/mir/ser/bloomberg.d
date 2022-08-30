/++
Authros: Ilya Yaroshenko
+/
module mir.ser.bloomberg;

import mir.bignum.low_level_view: BigIntView;
import mir.ion.exception: IonException;
public import mir.bloomberg.blpapi : BloombergElement = Element;
static import blpapi = mir.bloomberg.blpapi;

private alias validate = blpapi.validateBloombergErroCode;

private static immutable bloombergClobSerializationIsntImplemented = new IonException("Bloomberg CLOB serialization isn't implemented.");
private static immutable bloombergBlobSerializationIsntImplemented = new IonException("Bloomberg BLOB serialization isn't implemented.");

/++
Ion serialization back-end
+/
struct BloombergSerializer()
{
    import mir.format: stringBuf, getData;
    import mir.bignum.decimal: Decimal;
    import mir.bignum.integer: BigInt;
    import mir.ion.type_code;
    import mir.lob;
    import mir.timestamp;
    import mir.serde: SerdeTarget;
    import std.traits: isNumeric;

    BloombergElement* nextValue;

    BloombergElement* aggregateValue;

    typeof(stringBuf()) currentPartString;

    /// Mutable value used to choose format specidied or user-defined serialization specializations
    int serdeTarget = SerdeTarget.bloomberg;

    private uint valueIndex;

@safe pure:

    private const(char)* toScopeStringz(scope const(char)[] value) @trusted return scope nothrow
    {
        currentPartString.reset;
        currentPartString.put(value);
        currentPartString.put('\0');
        return currentPartString.data.ptr;
    }

    private void pushState(size_t state) @trusted
    {
        aggregateValue = cast(BloombergElement*)cast(void*)state;
        nextValue = null;
    }

    private BloombergElement* popState()
    {
        auto state = aggregateValue;
        aggregateValue = nextValue;
        nextValue = null;
        return state;
    }

    size_t stringBegin()
    {
        currentPartString.reset;
        return 0;
    }

    /++
    Puts string part. The implementation allows to split string unicode points.
    +/
    void putStringPart(scope const char[] value)
    {
        import mir.format: printEscaped, EscapeFormat;
        currentPartString.put(value);
    }

    void stringEnd(size_t) @trusted
    {
        if (currentPartString.length == 1)
        {
            blpapi.setValueChar(nextValue, *currentPartString.data.ptr, valueIndex).validate;
        }
        else
        {
            currentPartString.put('\0');
            blpapi.setValueString(nextValue, currentPartString.data.ptr, valueIndex).validate;
        }
    }

    private blpapi.Name* getName(scope const char* str)
    {
        if (auto name = blpapi.nameFindName(str))
            return name;
        return blpapi.nameCreate(str);
    }

    private blpapi.Name* getName(scope const char[] str)
    {
        return getName(toScopeStringz(str));
    }

    void putSymbolPtr(scope const char* value)
    {
        auto name = getName(value);
        blpapi.setValueFromName(nextValue, name, valueIndex).validate;
        blpapi.nameDestroy(name);
    }

    void putSymbol(scope const char[] value)
    {
        return putSymbolPtr(toScopeStringz(value));
    }

    size_t structBegin(size_t length = 0)
    {
        return cast(size_t) cast(const void*) popState;
    }

    void structEnd(size_t state)
    {
        pushState(state);
    }

    size_t listBegin(size_t length = 0)
    {
        valueIndex = uint.max;
        return 0;
    }

    void listEnd(size_t state)
    {
        valueIndex = 0;
        nextValue = null;
    }

    alias sexpBegin = listBegin;

    alias sexpEnd = listEnd;

    size_t annotationsBegin()
    {
        return cast(size_t)cast(const void*) aggregateValue;
    }

    void putAnnotationPtr(scope const char* value)
    {
        aggregateValue = nextValue;
        auto name = getName(value);
        blpapi.setChoice(nextValue, nextValue, null, name, 0).validate;
        blpapi.nameDestroy(name);
    }

    void putAnnotation(scope const char[] value) @trusted
    {
        putAnnotationPtr(toScopeStringz(value));
    }

    size_t annotationsEnd(size_t state) @trusted
    {
        aggregateValue = cast(BloombergElement*)cast(const void*) state;
        return 0;
    }

    size_t annotationWrapperBegin()
    {
        return 0;
    }

    void annotationWrapperEnd(size_t, size_t)
    {
    }

    void nextTopLevelValue()
    {
        static immutable exc = new IonException("Can't serialize to multiple Bloomberg Elements at once.");
        throw exc;
    }

    void putKeyPtr(scope const char* key)
    {
        nextValue = null;
        auto name = getName(key);
        blpapi.getElement(aggregateValue, nextValue, null, name).validate;
        blpapi.nameDestroy(name);
        assert(nextValue !is null);
    }

    void putKey(scope const char[] key)
    {
        putKeyPtr(toScopeStringz(key));
    }

    void putValue(Num)(const Num value)
        if (isNumeric!Num && !is(Num == enum))
    {
        import mir.internal.utility: isFloatingPoint;

        assert(nextValue);
        static if (isFloatingPoint!Num)
        {
            if (float(value) is value)
            {
                blpapi.setValueFloat32(nextValue, value, valueIndex).validate;
            }
            else
            {
                blpapi.setValueFloat64(nextValue, value, valueIndex).validate;
            }
        }
        else
        static if (is(Num == int) || Num.sizeof <= 2)
        {
            static if (is(Num == ulong))
            {
                if (value > long.max)
                {
                    static immutable exc = new SerdeException("BloombergSerializer: integer overflow");
                    throw exc;
                }
            }

            (cast(int) value == cast(long) value
                 ? blpapi.setValueInt32(nextValue, value, 0)
                 : blpapi.setValueInt64(nextValue, value, 0))
                 .validate;
        }
    }

    void putValue(W)(BigIntView!W view)
    {
        auto i = cast(long) view;
        if (view != i)
        {
            import mir.serde: SerdeException;
            static immutable exc = new SerdeException("BloombergSerializer: integer overflow");
            throw exc;
        }
        putValue(i);
    }

    void putValue(size_t size)(auto ref const BigInt!size num)
    {
        putValue(num.view);
    }

    void putValue(size_t size)(auto ref const Decimal!size num)
    {
        putValue(cast(double)num);
    }

    void putValue(typeof(null))
    {
        assert(nextValue);
    }

    /// ditto 
    void putNull(IonTypeCode code)
    {
        putValue(null);
    }

    void putValue(bool b)
    {
        assert(nextValue);
        blpapi.setValueBool(nextValue, b, valueIndex).validate;
    }

    void putValue(scope const char[] value)
    {
        auto state = stringBegin;
        putStringPart(value);
        stringEnd(state);
    }

    void putValue(Clob value)
    {
        throw bloombergClobSerializationIsntImplemented;
    }

    void putValue(Blob value)
    {
        throw bloombergBlobSerializationIsntImplemented;
    }

    void putValue(Timestamp value)
    {
        blpapi.HighPrecisionDatetime dt = value;
        blpapi.setValueHighPrecisionDatetime(nextValue, dt, valueIndex).validate;
    }

    void elemBegin()
    {
    }

    alias sexpElemBegin = elemBegin;
}

private static immutable excBytearray = new Exception("Mir Bloomberg: unexpected data type: bytearray");
private static immutable excCorrelationOd = new Exception("Mir Bloomberg: unexpected data type: correlation_id");

///
void serializeValue(S)(ref S serializer, const(BloombergElement)* value)
{
    import core.stdc.string: strlen;
    import mir.ion.type_code;
    import mir.timestamp: Timestamp;
    static import blpapi = mir.bloomberg.blpapi;
    import mir.bloomberg.blpapi: validate = validateBloombergErroCode;

    if (value is null || blpapi.isNull(value))
    {
        serializer.putValue(null);
        return;
    }

    auto isArray = blpapi.isArray(value);
    auto type = blpapi.datatype(value);
    size_t arrayLength = 1;
    typeof(serializer.listBegin(arrayLength)) arrayState;
    if (isArray)
    {
        arrayLength = blpapi.numValues(value);
        arrayState = serializer.listBegin(arrayLength);
        if (type == blpapi.DataType.choice || type == blpapi.DataType.sequence)
        {
            foreach(index; 0 .. arrayLength)
            {
                BloombergElement* v;
                blpapi.getValueAsElement(value, v, index).validate;
                serializer.elemBegin;
                serializer.serializeValue(v);
            }
            serializer.listEnd(arrayState);
            return;
        }
    }
    foreach(index; 0 .. arrayLength)
    {
        if (isArray)
            serializer.elemBegin;
        final switch (type)
        {
            case blpapi.DataType.null_:
                serializer.putValue(null);
                continue;
            case blpapi.DataType.bool_: {
                blpapi.Bool v;
                blpapi.getValueAsBool(value, v, index).validate;
                serializer.putValue(cast(bool)v);
                continue;
            }
            case blpapi.DataType.char_: {
                char[1] v;
                blpapi.getValueAsChar(value, v[0], index).validate;
                serializer.putValue(v[]);
                continue;
            }
            case blpapi.DataType.byte_:
            case blpapi.DataType.int32: {
                int v;
                blpapi.getValueAsInt32(value, v, index).validate;
                serializer.putValue(v);
                continue;
            }
            case blpapi.DataType.int64: {
                long v;
                blpapi.getValueAsInt64(value, v, index).validate;
                serializer.putValue(v);
                continue;
            }
            case blpapi.DataType.float32: {
                float v;
                blpapi.getValueAsFloat32(value, v, index).validate;
                serializer.putValue(v);
                continue;
            }
            case blpapi.DataType.decimal:
            case blpapi.DataType.float64: {
                double v;
                blpapi.getValueAsFloat64(value, v, index).validate;
                serializer.putValue(v);
                continue;
            }
            case blpapi.DataType.string: {
                const(char)* v;
                blpapi.getValueAsString(value, v, index).validate;
                serializer.putValue(v[0 .. (()@trusted => v.strlen)()]);
                continue;
            }
            case blpapi.DataType.date:
            case blpapi.DataType.time:
            case blpapi.DataType.datetime: {
                blpapi.HighPrecisionDatetime v;
                blpapi.getValueAsHighPrecisionDatetime(value, v, index).validate;
                if (v.parts)
                    serializer.putValue(cast(Timestamp)v);
                else
                    serializer.putNull(IonTypeCode.timestamp);
                continue;
            }
            case blpapi.DataType.enumeration: {
                const(char)* v;
                blpapi.getValueAsString(value, v, index).validate;
                if (v is null)
                    serializer.putNull(IonTypeCode.symbol);
                else
                    serializer.putSymbol(v[0 .. (()@trusted => v.strlen)()]);
                continue;
            }
            case blpapi.DataType.sequence: {
                auto length = blpapi.numElements(value);
                auto state = serializer.structBegin(length);
                foreach(i; 0 .. length)
                {
                    BloombergElement* v;
                    blpapi.getElementAt(value, v, i).validate;
                    blpapi.Name* name = blpapi.name(v);
                    const(char)* keyPtr = name ? blpapi.nameString(name) :  null;
                    auto key = keyPtr ? keyPtr[0 .. (()@trusted => keyPtr.strlen)()] : null;
                    serializer.putKey(key);
                    serializer.serializeValue(v);
                }
                serializer.structEnd(state);
                continue;
            }
            case blpapi.DataType.choice: {
                auto wrapperState = serializer.annotationWrapperBegin;
                do
                {
                    BloombergElement* v;
                    blpapi.getChoice(value, v).validate;
                    blpapi.Name* name = blpapi.name(v);
                    const(char)* annotationPtr = name ? blpapi.nameString(name) :  null;
                    auto annotation = annotationPtr ? annotationPtr[0 .. (()@trusted => annotationPtr.strlen)()] : null;
                    serializer.putAnnotation(annotation);
                    value = v;
                }
                while (blpapi.datatype(value) == blpapi.DataType.choice);
                auto annotationsEnd = serializer.annotationsEnd(wrapperState);
                serializer.serializeValue(value);
                serializer.annotationWrapperEnd(annotationsEnd, wrapperState);
                continue;
            }
            case blpapi.DataType.bytearray:
                throw excBytearray;
            case blpapi.DataType.correlation_id:
                throw excCorrelationOd;
        }
    }
    if (isArray)
    {
        serializer.listEnd(arrayState);
    }
}

unittest
{
    import mir.ser.bloomberg;
    import mir.ser.ion;
    import mir.ser.json;
    import mir.ser.text;
    import mir.ser.interfaces: SerializerWrapper;
    BloombergSerializer!() ser;
    BloombergElement* value;
    serializeValue(ser, value.init);
    scope wserializer = new SerializerWrapper!(typeof(ser))(ser);

    import mir.string_map;
    import mir.ser: serializeValue;
    const string[string] val;
    serializeValue(wserializer, val);
    auto text = value.serializeText;
    auto json = value.serializeJson;
    auto ion = value.serializeIon;
}
