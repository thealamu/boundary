package errors

type Info struct {
	Kind    Kind
	Message string
}

// errorCodeInfo provides a map of unique Codes (IDs) to their
// corresponding Kind and a default Message.
var errorCodeInfo = map[Code]Info{
	Unknown: {
		Message: "unknown",
		Kind:    Other,
	},
	InvalidParameter: {
		Message: "invalid parameter",
		Kind:    Parameter,
	},
	InvalidAddress: {
		Message: "invalid address",
		Kind:    Parameter,
	},
	InvalidFieldMask: {
		Message: "invalid field mask",
		Kind:    Parameter,
	},
	EmptyFieldMask: {
		Message: "empty field",
		Kind:    Parameter,
	},
	MissingScopeId: {
		Message: "missing scope id",
		Kind:    Parameter,
	},
	MissingPublicId: {
		Message: "missing public id",
		Kind:    Parameter,
	},
	MissingSetId: {
		Message: "missing set id",
		Kind:    Parameter,
	},
	MissingVersion: {
		Message: "missing version",
		Kind:    Parameter,
	},
	MissingCatalogId: {
		Message: "missing catalog id",
		Kind:    Parameter,
	},
	MissingHostIds: {
		Message: "missing host ids",
		Kind:    Parameter,
	},
	GenerateId: {
		Message: "failed to generate ID",
		Kind:    Parameter,
	},
	CheckConstraint: {
		Message: "constraint check failed",
		Kind:    Integrity,
	},
	NotNull: {
		Message: "must not be empty (null) violation",
		Kind:    Integrity,
	},
	NotUnique: {
		Message: "must be unique violation",
		Kind:    Integrity,
	},
	NotSpecificIntegrity: {
		Message: "Integrity violation without specific details",
		Kind:    Integrity,
	},
	MissingTable: {
		Message: "missing table",
		Kind:    Integrity,
	},
	RecordNotFound: {
		Message: "record not found",
		Kind:    Search,
	},
	MultipleRecords: {
		Message: "multiple records",
		Kind:    Search,
	},
}
