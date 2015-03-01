#![feature(io, path, fs)]
extern crate phf_codegen;

use std::env;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::path::AsPath;

// From http://www.postgresql.org/docs/9.2/static/errcodes-appendix.html
static SQLSTATES: &'static [(&'static str, &'static str)] = &[
        // Class 00 — Successful Completion
        ("00000", "SuccessfulCompletion"),

        // Class 01 — Warning
        ("01000", "Warning"),
        ("0100C", "DynamicResultSetsReturned"),
        ("01008", "ImplicitZeroBitPadding"),
        ("01003", "NullValueEliminatedInSetFunction"),
        ("01007", "PrivilegeNotGranted"),
        ("01006", "PrivilegeNotRevoked"),
        ("01004", "StringDataRightTruncationWarning"),
        ("01P01", "DeprecatedFeature"),

        // Class 02 — No Data
        ("02000", "NoData"),
        ("02001", "NoAdditionalDynamicResultSetsReturned"),

        // Class 03 — SQL Statement Not Yet Complete
        ("03000", "SqlStatementNotYetComplete"),

        // Class 08 — Connection Exception
        ("08000", "ConnectionException"),
        ("08003", "ConnectionDoesNotExist"),
        ("08006", "ConnectionFailure"),
        ("08001", "SqlclientUnableToEstablishSqlconnection"),
        ("08004", "SqlserverRejectedEstablishmentOfSqlconnection"),
        ("08007", "TransactionResolutionUnknown"),
        ("08P01", "ProtocolViolation"),

        // Class 09 — Triggered Action Exception
        ("09000", "TriggeredActionException"),

        // Class 0A — Feature Not Supported
        ("0A000", "FeatureNotSupported"),

        // Class 0B — Invalid Transaction Initiation
        ("0B000", "InvalidTransactionInitiation"),

        // Class 0F — Locator Exception
        ("0F000", "LocatorException"),
        ("0F001", "InvalidLocatorException"),

        // Class 0L — Invalid Grantor
        ("0L000", "InvalidGrantor"),
        ("0LP01", "InvalidGrantOperation"),

        // Class 0P — Invalid Role Specification
        ("0P000", "InvalidRoleSpecification"),

        // Class 0Z — Diagnostics Exception
        ("0Z000", "DiagnosticsException"),
        ("0Z002", "StackedDiagnosticsAccessedWithoutActiveHandler"),

        // Class 20 — Case Not Found
        ("20000", "CaseNotFound"),

        // Class 21 — Cardinality Violation
        ("21000", "CardinalityViolation"),

        // Class 22 — Data Exception
        ("22000", "DataException"),
        ("2202E", "ArraySubscriptError"),
        ("22021", "CharacterNotInRepertoire"),
        ("22008", "DatetimeFieldOverflow"),
        ("22012", "DivisionByZero"),
        ("22005", "ErrorInAssignment"),
        ("2200B", "EscapeCharacterConflict"),
        ("22022", "IndicatorOverflow"),
        ("22015", "IntervalFieldOverflow"),
        ("2201E", "InvalidArgumentForLogarithm"),
        ("22014", "InvalidArgumentForNtileFunction"),
        ("22016", "InvalidArgumentForNthValueFunction"),
        ("2201F", "InvalidArgumentForPowerFunction"),
        ("2201G", "InvalidArgumentForWidthBucketFunction"),
        ("22018", "InvalidCharacterValueForCast"),
        ("22007", "InvalidDatetimeFormat"),
        ("22019", "InvalidEscapeCharacter"),
        ("2200D", "InvalidEscapeOctet"),
        ("22025", "InvalidEscapeSequence"),
        ("22P06", "NonstandardUseOfEscapeCharacter"),
        ("22010", "InvalidIndicatorParameterValue"),
        ("22023", "InvalidParameterValue"),
        ("2201B", "InvalidRegularExpression"),
        ("2201W", "InvalidRowCountInLimitClause"),
        ("2201X", "InvalidRowCountInResultOffsetClause"),
        ("22009", "InvalidTimeZoneDisplacementValue"),
        ("2200C", "InvalidUseOfEscapeCharacter"),
        ("2200G", "MostSpecificTypeMismatch"),
        ("22004", "NullValueNotAllowedData"),
        ("22002", "NullValueNoIndicatorParameter"),
        ("22003", "NumericValueOutOfRange"),
        ("22026", "StringDataLengthMismatch"),
        ("22001", "StringDataRightTruncationException"),
        ("22011", "SubstringError"),
        ("22027", "TrimError"),
        ("22024", "UnterminatedCString"),
        ("2200F", "ZeroLengthCharacterString"),
        ("22P01", "FloatingPointException"),
        ("22P02", "InvalidTextRepresentation"),
        ("22P03", "InvalidBinaryRepresentation"),
        ("22P04", "BadCopyFileFormat"),
        ("22P05", "UntranslatableCharacter"),
        ("2200L", "NotAnXmlDocument"),
        ("2200M", "InvalidXmlDocument"),
        ("2200N", "InvalidXmlContent"),
        ("2200S", "InvalidXmlComment"),
        ("2200T", "InvalidXmlProcessingInstruction"),

        // Class 23 — Integrity Constraint Violation
        ("23000", "IntegrityConstraintViolation"),
        ("23001", "RestrictViolation"),
        ("23502", "NotNullViolation"),
        ("23503", "ForeignKeyViolation"),
        ("23505", "UniqueViolation"),
        ("23514", "CheckViolation"),
        ("32P01", "ExclusionViolation"),

        // Class 24 — Invalid Cursor State
        ("24000", "InvalidCursorState"),

        // Class 25 — Invalid Transaction State
        ("25000", "InvalidTransactionState"),
        ("25001", "ActiveSqlTransaction"),
        ("25002", "BranchTransactionAlreadyActive"),
        ("25008", "HeldCursorRequiresSameIsolationLevel"),
        ("25003", "InappropriateAccessModeForBranchTransaction"),
        ("25004", "InappropriateIsolationLevelForBranchTransaction"),
        ("25005", "NoActiveSqlTransactionForBranchTransaction"),
        ("25006", "ReadOnlySqlTransaction"),
        ("25007", "SchemaAndDataStatementMixingNotSupported"),
        ("25P01", "NoActiveSqlTransaction"),
        ("25P02", "InFailedSqlTransaction"),

        // Class 26 — Invalid SQL Statement Name
        ("26000", "InvalidSqlStatementName"),

        // Class 27 — Triggered Data Change Violation
        ("27000", "TriggeredDataChangeViolation"),

        // Class 28 — Invalid Authorization Specification
        ("28000", "InvalidAuthorizationSpecification"),
        ("28P01", "InvalidPassword"),

        // Class 2B — Dependent Privilege Descriptors Still Exist
        ("2B000", "DependentPrivilegeDescriptorsStillExist"),
        ("2BP01", "DependentObjectsStillExist"),

        // Class 2D — Invalid Transaction Termination
        ("2D000", "InvalidTransactionTermination"),

        // Class 2F — SQL Routine Exception
        ("2F000", "SqlRoutineException"),
        ("2F005", "FunctionExecutedNoReturnStatement"),
        ("2F002", "ModifyingSqlDataNotPermittedSqlRoutine"),
        ("2F003", "ProhibitedSqlStatementAttemptedSqlRoutine"),
        ("2F004", "ReadingSqlDataNotPermittedSqlRoutine"),

        // Class 34 — Invalid Cursor Name
        ("34000", "InvalidCursorName"),

        // Class 38 — External Routine Exception
        ("38000", "ExternalRoutineException"),
        ("38001", "ContainingSqlNotPermitted"),
        ("38002", "ModifyingSqlDataNotPermittedExternalRoutine"),
        ("38003", "ProhibitedSqlStatementAttemptedExternalRoutine"),
        ("38004", "ReadingSqlDataNotPermittedExternalRoutine"),

        // Class 39 — External Routine Invocation Exception
        ("39000", "ExternalRoutineInvocationException"),
        ("39001", "InvalidSqlstateReturned"),
        ("39004", "NullValueNotAllowedExternalRoutine"),
        ("39P01", "TriggerProtocolViolated"),
        ("39P02", "SrfProtocolViolated"),

        // Class 3B — Savepoint Exception
        ("3B000", "SavepointException"),
        ("3B001", "InvalidSavepointException"),

        // Class 3D — Invalid Catalog Name
        ("3D000", "InvalidCatalogName"),

        // Class 3F — Invalid Schema Name
        ("3F000", "InvalidSchemaName"),

        // Class 40 — Transaction Rollback
        ("40000", "TransactionRollback"),
        ("40002", "TransactionIntegrityConstraintViolation"),
        ("40001", "SerializationFailure"),
        ("40003", "StatementCompletionUnknown"),
        ("40P01", "DeadlockDetected"),

        // Class 42 — Syntax Error or Access Rule Violation
        ("42000", "SyntaxErrorOrAccessRuleViolation"),
        ("42601", "SyntaxError"),
        ("42501", "InsufficientPrivilege"),
        ("42846", "CannotCoerce"),
        ("42803", "GroupingError"),
        ("42P20", "WindowingError"),
        ("42P19", "InvalidRecursion"),
        ("42830", "InvalidForeignKey"),
        ("42602", "InvalidName"),
        ("42622", "NameTooLong"),
        ("42939", "ReservedName"),
        ("42804", "DatatypeMismatch"),
        ("42P18", "IndeterminateDatatype"),
        ("42P21", "CollationMismatch"),
        ("42P22", "IndeterminateCollation"),
        ("42809", "WrongObjectType"),
        ("42703", "UndefinedColumn"),
        ("42883", "UndefinedFunction"),
        ("42P01", "UndefinedTable"),
        ("42P02", "UndefinedParameter"),
        ("42704", "UndefinedObject"),
        ("42701", "DuplicateColumn"),
        ("42P03", "DuplicateCursor"),
        ("42P04", "DuplicateDatabase"),
        ("42723", "DuplicateFunction"),
        ("42P05", "DuplicatePreparedStatement"),
        ("42P06", "DuplicateSchema"),
        ("42P07", "DuplicateTable"),
        ("42712", "DuplicateAliaas"),
        ("42710", "DuplicateObject"),
        ("42702", "AmbiguousColumn"),
        ("42725", "AmbiguousFunction"),
        ("42P08", "AmbiguousParameter"),
        ("42P09", "AmbiguousAlias"),
        ("42P10", "InvalidColumnReference"),
        ("42611", "InvalidColumnDefinition"),
        ("42P11", "InvalidCursorDefinition"),
        ("42P12", "InvalidDatabaseDefinition"),
        ("42P13", "InvalidFunctionDefinition"),
        ("42P14", "InvalidPreparedStatementDefinition"),
        ("42P15", "InvalidSchemaDefinition"),
        ("42P16", "InvalidTableDefinition"),
        ("42P17", "InvalidObjectDefinition"),

        // Class 44 — WITH CHECK OPTION Violation
        ("44000", "WithCheckOptionViolation"),

        // Class 53 — Insufficient Resources
        ("53000", "InsufficientResources"),
        ("53100", "DiskFull"),
        ("53200", "OutOfMemory"),
        ("53300", "TooManyConnections"),
        ("53400", "ConfigurationLimitExceeded"),

        // Class 54 — Program Limit Exceeded
        ("54000", "ProgramLimitExceeded"),
        ("54001", "StatementTooComplex"),
        ("54011", "TooManyColumns"),
        ("54023", "TooManyArguments"),

        // Class 55 — Object Not In Prerequisite State
        ("55000", "ObjectNotInPrerequisiteState"),
        ("55006", "ObjectInUse"),
        ("55P02", "CantChangeRuntimeParam"),
        ("55P03", "LockNotAvailable"),

        // Class 57 — Operator Intervention
        ("57000", "OperatorIntervention"),
        ("57014", "QueryCanceled"),
        ("57P01", "AdminShutdown"),
        ("57P02", "CrashShutdown"),
        ("57P03", "CannotConnectNow"),
        ("57P04", "DatabaseDropped"),

        // Class 58 — System Error
        ("58000", "SystemError"),
        ("58030", "IoError"),
        ("58P01", "UndefinedFile"),
        ("58P02", "DuplicateFile"),

        // Class F0 — Configuration File Error
        ("F0000", "ConfigFileError"),
        ("F0001", "LockFileExists"),

        // Class HV — Foreign Data Wrapper Error (SQL/MED)
        ("HV000", "FdwError"),
        ("HV005", "FdwColumnNameNotFound"),
        ("HV002", "FdwDynamicParameterValueNeeded"),
        ("HV010", "FdwFunctionSequenceError"),
        ("HV021", "FdwInconsistentDescriptorInformation"),
        ("HV024", "FdwInvalidAttributeValue"),
        ("HV007", "FdwInvalidColumnName"),
        ("HV008", "FdwInvalidColumnNumber"),
        ("HV004", "FdwInvalidDataType"),
        ("HV006", "FdwInvalidDataTypeDescriptors"),
        ("HV091", "FdwInvalidDescriptorFieldIdentifier"),
        ("HV00B", "FdwInvalidHandle"),
        ("HV00C", "FdwInvalidOptionIndex"),
        ("HV00D", "FdwInvalidOptionName"),
        ("HV090", "FdwInvalidStringLengthOrBufferLength"),
        ("HV00A", "FdwInvalidStringFormat"),
        ("HV009", "FdwInvalidUseOfNullPointer"),
        ("HV014", "FdwTooManyHandles"),
        ("HV001", "FdwOutOfMemory"),
        ("HV00P", "FdwNoSchemas"),
        ("HV00J", "FdwOptionNameNotFound"),
        ("HV00K", "FdwReplyHandle"),
        ("HV00Q", "FdwSchemaNotFound"),
        ("HV00R", "FdwTableNotFound"),
        ("HV00L", "FdwUnableToCreateExcecution"),
        ("HV00M", "FdwUnableToCreateReply"),
        ("HV00N", "FdwUnableToEstablishConnection"),

        // Class P0 — PL/pgSQL Error
        ("P0000", "PlpgsqlError"),
        ("P0001", "RaiseException"),
        ("P0002", "NoDataFound"),
        ("P0003", "TooManyRows"),

        // Class XX — Internal Error
        ("XX000", "InternalError"),
        ("XX001", "DataCorrupted"),
        ("XX002", "IndexCorrupted"),
    ];

fn main() {
    let path = env::var_os("OUT_DIR").unwrap().as_path().join("sqlstate.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());

    make_enum(&mut file);
    make_map(&mut file);
    make_impl(&mut file);
    make_debug(&mut file);
}

fn make_enum(file: &mut BufWriter<File>) {
    write!(file,
r#"/// SQLSTATE error codes
#[derive(PartialEq, Eq, Clone)]
#[allow(missing_docs)]
pub enum SqlState {{
"#
           ).unwrap();

    for &(_, variant) in SQLSTATES {
        write!(file,
"    {},\n",
               variant).unwrap();
    }

    write!(file,
"    Unknown(String)
}}
"
           ).unwrap();
}

fn make_map(file: &mut BufWriter<File>) {
    write!(file, "static SQLSTATE_MAP: phf::Map<&'static str, SqlState> = ").unwrap();
    let mut builder = phf_codegen::Map::new();
    for &(code, variant) in SQLSTATES {
        builder.entry(code, &format!("SqlState::{}", variant));
    }
    builder.build(file).unwrap();
    write!(file, ";\n").unwrap();
}

fn make_impl(file: &mut BufWriter<File>) {
    write!(file, r#"
impl SqlState {{
    /// Creates a `SqlState` from its error code.
    pub fn from_code(s: String) -> SqlState {{
        match SQLSTATE_MAP.get(&*s) {{
            Some(state) => state.clone(),
            None => SqlState::Unknown(s)
        }}
    }}

    /// Returns the error code corresponding to the `SqlState`.
    pub fn code(&self) -> &str {{
        match *self {{"#
           ).unwrap();

    for &(code, variant) in SQLSTATES {
        write!(file, r#"
            SqlState::{} => "{}","#,
               variant, code).unwrap();
    }

    write!(file, r#"
            SqlState::Unknown(ref s) => &**s,
        }}
    }}
}}
"#
           ).unwrap();
}

fn make_debug(file: &mut BufWriter<File>) {
    write!(file, r#"
impl fmt::Debug for SqlState {{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {{
        let s = match *self {{"#
           ).unwrap();

    for &(_, variant) in SQLSTATES {
        write!(file, r#"
            SqlState::{0} => "{0}","#,
               variant).unwrap();
    }

    write!(file, r#"
            SqlState::Unknown(ref s) => return write!(fmt, "Unknown({{:?}})", s),
        }};
        fmt.write_str(s)
    }}
}}
"#
           ).unwrap();
}
