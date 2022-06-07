module mir.bloomberg.blpapi;

import mir.timestamp: Timestamp;
import mir.algebraic: Nullable;

alias BloombergStreamWriter = extern(C) int function(const(char)* data, int length, void* stream);

deprecated("Please use a local alias or IonAlgebraic instead .")
alias BloombergAlgebraic = Nullable!(
    bool,
    long,
    double,
    string,
    Timestamp);

enum DataType
{
    null_,
    /// Bool
    bool_          = 1,
    /// Char
    char_          = 2,
    /// Unsigned 8 bit value
    byte_          = 3,
    /// 32 bit Integer
    int32          = 4,
    /// 64 bit Integer
    int64          = 5,
    /// 32 bit Floating point - IEEE
    float32        = 6,
    /// 64 bit Floating point - IEEE
    float64        = 7,
    /// ASCIIZ string
    string         = 8,
    /// Opaque binary data
    bytearray      = 9,
    /// Date
    date           = 10,
    /// Timestamp
    time           = 11,
    decimal        = 12,
    /// Date and time
    datetime       = 13,
    /// An opaque enumeration
    enumeration    = 14,
    /// Sequence type
    sequence       = 15,
    /// Choice type
    choice         = 16,
    /// Used for some internal messages
    correlation_id = 17,
}

enum DatetimeParts : ubyte
{
    year         = 0x1,
    month        = 0x2,
    day          = 0x4,
    offset       = 0x8,
    hours        = 0x10,
    minutes      = 0x20,
    seconds      = 0x40,
    fracseconds  = 0x80,
}

struct ErrorInfo
{
    int   exceptionClass;
    char[256]  _description = '\0';

    inout(char)[] description() @trusted pure nothrow @nogc inout return scope
    {
        import core.stdc.string: strlen;
        return _description[0 .. (&this._description[0]).strlen];
    }
}

enum DatePart = DatetimeParts.year | DatetimeParts.month | DatetimeParts.day;

enum TimePart = DatetimeParts.hours | DatetimeParts.minutes | DatetimeParts.seconds;

enum TimeFracsecondsPart = TimePart | DatetimeParts.fracseconds;

alias Bool = int;

struct Name;

struct Element;

struct Datetime
{
    /// bitmask of date/time parts that are set
    ubyte  parts;
    ubyte  hours;
    ubyte  minutes;
    ubyte  seconds;
    ushort milliseconds;
    ubyte  month;
    ubyte  day;
    ushort year;
    /// (signed) minutes ahead of UTC
    short  offset;

    ///
    this(
        ubyte  parts,
        ubyte  hours,
        ubyte  minutes,
        ubyte  seconds,
        ushort milliseconds,
        ubyte  month,
        ubyte  day,
        ushort year,
        short  offset,
    ) {
        this.parts = parts;
        this.hours = hours;
        this.minutes = minutes;
        this.seconds = seconds;
        this.milliseconds = milliseconds;
        this.month = month;
        this.day = day;
        this.year = year;
        this.offset = offset;
    }

    /// Construct from $(MREF mir,timestamp).
    this(Timestamp timestamp) @safe pure nothrow @nogc
    {
        this = HighPrecisionDatetime(timestamp).datetime;
    }

    /// Converts `Datetime` to $(MREF mir,timestamp).
    Timestamp asTimestamp() @safe pure nothrow @nogc const @property
    {
        return HighPrecisionDatetime(this).asTimestamp;
    }

    alias opCast(T : Timestamp) = asTimestamp;

    bool isOnlyTime() @safe pure nothrow @nogc const @property
    {
        return (parts & DatetimeParts.year) == 0;
    }
}

struct HighPrecisionDatetime {
    Datetime datetime;

    alias datetime this;

    /++
    picosecond offset into current
    *millisecond* i.e. the picosecond offset
    into the current full second is
    '1000000000LL * milliseconds + picoseconds'
    +/
    uint picoseconds;

    this(Datetime datetime, uint picoseconds = 0) @safe pure nothrow @nogc
    {
        this.datetime = datetime;
        this.picoseconds = picoseconds;
    }

    /// Construct from $(MREF mir,timestamp).
    this(Timestamp timestamp) @safe pure nothrow @nogc
    {
        if (timestamp.offset)
        {
            parts |= DatetimeParts.offset;
            offset = timestamp.offset;
            timestamp.addMinutes(timestamp.offset);
        }
        final switch (timestamp.precision)
        {
            case Timestamp.Precision.fraction: {
                parts |= DatetimeParts.fracseconds;
                auto exp = timestamp.fractionExponent;
                auto coeff = timestamp.fractionCoefficient;
                while(exp > -12)
                {
                    exp--;
                    coeff *= 10;
                }
                picoseconds = cast(uint) (coeff % 1000000000u);
                milliseconds = cast(ushort) (coeff / 1000000000u);
                goto case;
            }
            case Timestamp.Precision.second:
                parts |= DatetimeParts.seconds;
                seconds = timestamp.second;
                goto case;
            case Timestamp.Precision.minute:
                parts |= DatetimeParts.minutes;
                parts |= DatetimeParts.hours;
                minutes = timestamp.minute;
                hours = timestamp.hour;
                if (timestamp.day == 0) //
                    return;
                goto case;
            case Timestamp.Precision.day:
                parts |= DatetimeParts.day;
                day = timestamp.day;
                goto case;
            case Timestamp.Precision.month:
                parts |= DatetimeParts.month;
                month = timestamp.month;
                goto case;
            case Timestamp.Precision.year:
                parts |= DatetimeParts.year;
                year = timestamp.year;
        }
    }

    /// Converts `Datetime` to $(MREF mir,timestamp).
    Timestamp asTimestamp() @safe pure nothrow @nogc const @property
    {
        Timestamp ret;
        if (parts & DatetimeParts.year)
        {
            ret.year = year;
            ret.precision = Timestamp.Precision.year;
        }
        if (parts & DatetimeParts.month)
        {
            ret.month = month;
            ret.precision = Timestamp.Precision.month;
        }
        if (parts & DatetimeParts.day)
        {
            ret.day = day;
            ret.precision = Timestamp.Precision.day;
        }
        if (parts & DatetimeParts.hours)
        {
            ret.hour = hours;
            ret.precision = Timestamp.Precision.minute;
        }
        if (parts & DatetimeParts.minutes)
        {
            ret.minute = minutes;
            ret.precision = Timestamp.Precision.minute;
        }
        if (parts & DatetimeParts.seconds)
        {
            ret.second = seconds;
            ret.precision = Timestamp.Precision.second;
        }
        if (parts & DatetimeParts.fracseconds)
        {
            ret.fractionExponent = -12;
            ret.fractionCoefficient = 1000000000UL * milliseconds + picoseconds;
            ret.precision = Timestamp.Precision.fraction;
        }
        if (parts & DatetimeParts.offset && offset && ret.precision >= Timestamp.Precision.minute)
        {
            ret.addMinutes(cast(short)-int(ret.offset));
        }
        return ret;
    }

    alias opCast(T : Timestamp) = asTimestamp;
}

unittest
{
    auto tests = [
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 11, 1, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 10, 29, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 11, 2, 2020, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 6, 2, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 10, 29, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 10, 29, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 10, 29, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(112, 7, 0, 0, 0, 1, 1, 1, 0), 0),
        HighPrecisionDatetime(Datetime(112, 6, 59, 59, 0, 1, 1, 1, 0), 0),
        HighPrecisionDatetime(Datetime(112, 16, 59, 59, 0, 1, 1, 1, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 10, 29, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 11, 1, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 10, 29, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 3, 15, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 11, 2, 2020, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 3, 23, 2020, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 3, 23, 2020, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 10, 29, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 10, 29, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 10, 29, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(7, 0, 0, 0, 0, 10, 29, 2021, 0), 0),
        HighPrecisionDatetime(Datetime(112, 9, 30, 0, 0, 1, 1, 1, 0), 0),
        HighPrecisionDatetime(Datetime(112, 16, 30, 0, 0, 1, 1, 1, 0), 0),
        HighPrecisionDatetime(Datetime(112, 20, 4, 0, 0, 1, 1, 1, 0), 0),
        HighPrecisionDatetime(Datetime(240, 20, 4, 0, 0, 1, 1, 1, 0), 0),
        HighPrecisionDatetime(Datetime(240, 12, 38, 48, 0, 1, 1, 1, 0), 0),
        HighPrecisionDatetime(Datetime(240, 13, 5, 3, 0, 1, 1, 1, 0), 0),
    ];

    foreach (test; tests)
    {
        import mir.ser.ion;
        import mir.deser.ion;
        auto ts = test.asTimestamp;
        assert(ts.serializeIon.deserializeIon!Timestamp == ts);
    }
}

@safe pure // @nogc
validateBloombergErroCode()(
    int errorCode,
    string file = __FILE__,
    size_t line = __LINE__)
{
    import mir.ion.exception: IonException, IonMirException;
    if (errorCode)
    {
        // static if (__traits(compiles, () @nogc { throw new Exception(""); }))
        // {
            ErrorInfo info;
            getErrorInfo(info, errorCode);
            throw new IonMirException(info.description, file, line);
        // }
        // else
        // {
        //     static immutable exc = new IonException("Exception thrown in bloomberg API: add DIP1008 for better error messages.");
        //     throw exc;
        // }
    }
}

@safe pure nothrow @nogc extern(System):

alias getErrorInfo = blpapi_getErrorInfo;
int blpapi_getErrorInfo(scope ref ErrorInfo buffer, int errorCode);

alias nameCreate = blpapi_Name_create;
Name* blpapi_Name_create(
    scope const(char)* nameString);

alias nameDestroy = blpapi_Name_destroy;
void blpapi_Name_destroy(
    Name* name);

alias nameDuplicate = blpapi_Name_duplicate;
Name* blpapi_Name_duplicate(
    scope const Name* src);

alias nameEqualsStr = blpapi_Name_equalsStr;
int blpapi_Name_equalsStr(
    scope const Name* name,
    const char *string);

alias nameString = blpapi_Name_string;
const(char)* blpapi_Name_string(
    scope const Name* name);

alias nameLength = blpapi_Name_length;
size_t blpapi_Name_length(
    scope const Name* name);

alias nameFindName = blpapi_Name_findName;
Name* blpapi_Name_findName(
    scope const(char)* nameString);

int blpapi_Element_print(
    const Element* element,
    BloombergStreamWriter streamWriter,
    void *stream,
    int level,
    int spacesPerLevel);

alias print = blpapi_Element_print;

alias name = blpapi_Element_name;
Name* blpapi_Element_name(const Element *element);

alias nameString = blpapi_Element_nameString;
const(char)* blpapi_Element_nameString(const Element *element);

alias datatype = blpapi_Element_datatype;
DataType blpapi_Element_datatype (
    const(Element)* element);

alias isComplexType = blpapi_Element_isComplexType;
int blpapi_Element_isComplexType(
    const(Element)* element);

alias isArray = blpapi_Element_isArray;
int blpapi_Element_isArray(
    const(Element)* element);

alias isReadOnly = blpapi_Element_isReadOnly;
int blpapi_Element_isReadOnly(
    const(Element)* element);

alias numValues = blpapi_Element_numValues;
size_t blpapi_Element_numValues(
    const(Element)* element);

alias numElements = blpapi_Element_numElements;
size_t blpapi_Element_numElements(
    const(Element)* element);

alias isNullValue = blpapi_Element_isNullValue;
int blpapi_Element_isNullValue(
    const(Element)* element,
    size_t position);

alias isNull = blpapi_Element_isNull;
int blpapi_Element_isNull(
    const(Element)* element);

alias getElementAt = blpapi_Element_getElementAt;
int blpapi_Element_getElementAt(
    const(Element)* element,
    scope ref Element *result,
    size_t position);

alias getElement = blpapi_Element_getElement;
int blpapi_Element_getElement(
    const Element *element,
    scope ref Element *result,
    const(char)* nameString,
    const Name *name);

alias hasElement = blpapi_Element_hasElement;
int blpapi_Element_hasElement(
    const Element *element,
    const(char)* nameString,
    const Name *name);

alias hasElementEx = blpapi_Element_hasElementEx;
int blpapi_Element_hasElementEx(
    const Element *element,
    const(char)* nameString,
    const Name *name,
    int excludeNullElements,
    int reserved);

alias getValueAsBool = blpapi_Element_getValueAsBool;
int blpapi_Element_getValueAsBool(
    const Element *element,
    scope ref Bool buffer,
    size_t index);

alias getValueAsChar = blpapi_Element_getValueAsChar;
int blpapi_Element_getValueAsChar(
    const Element *element,
    scope ref char buffer,
    size_t index);

alias getValueAsInt32 = blpapi_Element_getValueAsInt32;
int blpapi_Element_getValueAsInt32(
    const Element *element,
    scope ref int buffer,
    size_t index);

alias getValueAsInt64 = blpapi_Element_getValueAsInt64;
int blpapi_Element_getValueAsInt64(
    const Element *element,
    scope ref long buffer,
    size_t index);

alias getValueAsFloat32 = blpapi_Element_getValueAsFloat32;
int blpapi_Element_getValueAsFloat32(
    const Element *element,
    scope ref float buffer,
    size_t index);

alias getValueAsFloat64 = blpapi_Element_getValueAsFloat64;
int blpapi_Element_getValueAsFloat64(
    const Element *element,
    scope ref double buffer,
    size_t index);

alias getValueAsString = blpapi_Element_getValueAsString;
int blpapi_Element_getValueAsString(
    const Element *element,
    scope ref const char *buffer,
    size_t index);

alias getValueAsDatetime = blpapi_Element_getValueAsDatetime;
int blpapi_Element_getValueAsDatetime(
    const Element *element,
    scope ref Datetime buffer,
    size_t index);

alias getValueAsHighPrecisionDatetime = blpapi_Element_getValueAsHighPrecisionDatetime;
int blpapi_Element_getValueAsHighPrecisionDatetime(
    const Element *element,
    scope ref HighPrecisionDatetime buffer,
    size_t index);

alias getValueAsElement = blpapi_Element_getValueAsElement;
int blpapi_Element_getValueAsElement(
    const Element *element,
    scope ref Element *buffer,
    size_t index);

alias getValueAsName = blpapi_Element_getValueAsName;
int blpapi_Element_getValueAsName(
    const Element *element,
    scope Name* *buffer,
    size_t index);

alias getChoice = blpapi_Element_getChoice;
int blpapi_Element_getChoice(
    const Element *element,
    scope ref Element *result);

alias setValueBool = blpapi_Element_setValueBool;
int blpapi_Element_setValueBool(
    Element *element,
    Bool value,
    size_t index);

alias setValueChar = blpapi_Element_setValueChar;
int blpapi_Element_setValueChar(
    Element *element,
    char value,
    size_t index);

alias setValueInt32 = blpapi_Element_setValueInt32;
int blpapi_Element_setValueInt32(
    Element *element,
    int value,
    size_t index);

alias setValueInt64 = blpapi_Element_setValueInt64;
int blpapi_Element_setValueInt64(
    Element *element,
    long value,
    size_t index);

alias setValueFloat32 = blpapi_Element_setValueFloat32;
int blpapi_Element_setValueFloat32(
    Element *element,
    float value,
    size_t index);

alias setValueFloat64 = blpapi_Element_setValueFloat64;
int blpapi_Element_setValueFloat64(
    Element *element,
    double value,
    size_t index);

alias setValueString = blpapi_Element_setValueString;
int blpapi_Element_setValueString(
    Element *element,
    const char *value,
    size_t index);

alias setValueDatetime = blpapi_Element_setValueDatetime;
int blpapi_Element_setValueDatetime(
    Element *element,
    scope ref const Datetime value,
    size_t index);

alias setValueHighPrecisionDatetime = blpapi_Element_setValueHighPrecisionDatetime;
int blpapi_Element_setValueHighPrecisionDatetime(
    Element *element,
    scope ref const HighPrecisionDatetime value,
    size_t index);

alias setValueFromElement = blpapi_Element_setValueFromElement;
int blpapi_Element_setValueFromElement(
    Element *element,
    Element *value,
    size_t index);

alias setValueFromName = blpapi_Element_setValueFromName;
int blpapi_Element_setValueFromName (
    Element *element,
    const Name *value,
    size_t index);

alias setElementBool = blpapi_Element_setElementBool;
int blpapi_Element_setElementBool(
    Element *element,
    const(char)* nameString,
    const Name* name,
    Bool value);

alias setElementChar = blpapi_Element_setElementChar;
int blpapi_Element_setElementChar(
    Element *element,
    const(char)* nameString,
    const Name* name,
    char value);

alias setElementInt32 = blpapi_Element_setElementInt32;
int blpapi_Element_setElementInt32(
    Element *element,
    const(char)* nameString,
    const Name* name,
    int value);

alias setElementInt64 = blpapi_Element_setElementInt64;
int blpapi_Element_setElementInt64(
    Element *element,
    const(char)* nameString,
    const Name* name,
    long value);

alias setElementFloat32 = blpapi_Element_setElementFloat32;
int blpapi_Element_setElementFloat32(
    Element *element,
    const(char)* nameString,
    const Name* name,
    float value);

alias setElementFloat64 = blpapi_Element_setElementFloat64;
int blpapi_Element_setElementFloat64(
    Element *element,
    const(char)* nameString,
    const Name* name,
    double value);

alias setElementString = blpapi_Element_setElementString;
int blpapi_Element_setElementString(
    Element *element,
    const char *nameString,
    const Name* name,
    const char *value);

alias setElementDatetime = blpapi_Element_setElementDatetime;
int blpapi_Element_setElementDatetime(
    Element *element,
    const(char)* nameString,
    const Name* name,
    scope ref const Datetime value);

alias setElementHighPrecisionDatetime = blpapi_Element_setElementHighPrecisionDatetime;
int blpapi_Element_setElementHighPrecisionDatetime(
    Element *element,
    const char *nameString,
    const Name *name,
    scope ref const HighPrecisionDatetime value);

alias setElementFromField = blpapi_Element_setElementFromField;
int blpapi_Element_setElementFromField(
    Element *element,
    const(char)* nameString,
    const Name* name,
    Element *sourcebuffer);

alias setElementFromName = blpapi_Element_setElementFromName;
int blpapi_Element_setElementFromName (
    Element *element,
    const(char)* elementName,
    const Name* name,
    const Name *buffer);

alias appendElement = blpapi_Element_appendElement;
int blpapi_Element_appendElement (
    Element *element,
    scope ref Element *appendedElement);

alias setChoice = blpapi_Element_setChoice;
int blpapi_Element_setChoice (
    Element *element,
    scope ref Element *resultElement,
    const(char)* nameCstr,
    const Name* name,
    size_t index);
