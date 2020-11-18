package static

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/boundary/internal/db"
	dbcommon "github.com/hashicorp/boundary/internal/db/common"
	"github.com/hashicorp/boundary/internal/errors"
	"github.com/hashicorp/boundary/internal/kms"
	"github.com/hashicorp/boundary/internal/oplog"
)

// CreateHost inserts h into the repository and returns a new Host
// containing the host's PublicId. h is not changed. h must contain a valid
// CatalogId. h must not contain a PublicId. The PublicId is generated and
// assigned by this method. opt is ignored.
//
// h must contain a valid Address.
//
// Both h.Name and h.Description are optional. If h.Name is set, it must be
// unique within h.CatalogId.
func (r *Repository) CreateHost(ctx context.Context, scopeId string, h *Host, opt ...Option) (*Host, error) {
	if h == nil {
		return nil, errors.New(errors.InvalidParameter, "Uk0vJrn5BP", errors.WithMsg("no static host"))
	}
	if h.Host == nil {
		return nil, errors.New(errors.InvalidParameter, "yzBIsas4id", errors.WithMsg("no embedded host"))
	}
	if h.CatalogId == "" {
		return nil, errors.New(errors.MissingCatalogId, "afNPD3klN6")
	}
	if h.PublicId != "" {
		return nil, errors.New(errors.InvalidParameter, "KGaXoHSCDQ", errors.WithMsg("public id not empty"))
	}
	if scopeId == "" {
		return nil, errors.New(errors.MissingScopeId, "IWwJEBJpMS")
	}
	h.Address = strings.TrimSpace(h.Address)
	if len(h.Address) < MinHostAddressLength || len(h.Address) > MaxHostAddressLength {
		return nil, errors.New(errors.InvalidAddress, "oTmHplf1VJ")
	}
	h = h.clone()

	opts := getOpts(opt...)

	if opts.withPublicId != "" {
		if !strings.HasPrefix(opts.withPublicId, HostPrefix+"_") {
			return nil, errors.New(
				errors.InvalidParameter,
				"fVA68Amp1G",
				errors.WithMsg(
					fmt.Sprintf("passed-in public ID %q has wrong prefix, should be %q",
						opts.withPublicId,
						HostPrefix),
				))
		}
		h.PublicId = opts.withPublicId
	} else {
		id, err := newHostId()
		if err != nil {
			return nil, errors.Wrap(err, "6wPMpwqf8k")
		}
		h.PublicId = id
	}

	oplogWrapper, err := r.kms.GetWrapper(ctx, scopeId, kms.KeyPurposeOplog)
	if err != nil {
		return nil, errors.Wrap(err, "7RQyYaqQX2", errors.WithMsg("unable to get oplog wrapper"))
	}

	var newHost *Host
	_, err = r.writer.DoTx(ctx, db.StdRetryCnt, db.ExpBackoff{},
		func(_ db.Reader, w db.Writer) error {
			newHost = h.clone()
			return w.Create(ctx, newHost, db.WithOplog(oplogWrapper, h.oplog(oplog.OpType_OP_TYPE_CREATE)))
		},
	)

	if err != nil {
		if dErr := errors.Convert(err, "yyg1sVUuzo"); dErr != nil {
			return nil, dErr
		}
		return nil, errors.New(
			errors.Unknown,
			"mEN8tF6Zbr",
			errors.WithMsg(fmt.Sprintf("catalog: %s: %q", h.CatalogId, h.Name)),
			errors.WithWrap(err),
		)
	}
	return newHost, nil
}

// UpdateHost updates the repository entry for h.PublicId with the values
// in h for the fields listed in fieldMaskPaths. It returns a new Host
// containing the updated values and a count of the number of records
// updated. h is not changed.
//
// h must contain a valid PublicId. Only h.Name, h.Description, and
// h.Address can be updated. If h.Name is set to a non-empty string, it
// must be unique within h.CatalogId. If h.Address is set, it must contain
// a valid address.
//
// An attribute of h will be set to NULL in the database if the attribute
// in h is the zero value and it is included in fieldMaskPaths.
func (r *Repository) UpdateHost(ctx context.Context, scopeId string, h *Host, version uint32, fieldMaskPaths []string, opt ...Option) (*Host, int, error) {
	if h == nil {
		return nil, db.NoRowsAffected, errors.New(errors.InvalidParameter, "h4qvnejlc5", errors.WithMsg("no static host"))
	}
	if h.Host == nil {
		return nil, db.NoRowsAffected, errors.New(errors.InvalidParameter, "Rrs2iswKXI", errors.WithMsg("no embedded host"))
	}
	if h.PublicId == "" {
		return nil, db.NoRowsAffected, errors.New(errors.MissingPublicId, "Jb28DGty13")
	}
	if version == 0 {
		return nil, db.NoRowsAffected, errors.New(errors.MissingVersion, "hvAqt1UJo6")
	}
	if scopeId == "" {
		return nil, db.NoRowsAffected, errors.New(errors.MissingScopeId, "jqU4qoUlBv")
	}

	for _, f := range fieldMaskPaths {
		switch {
		case strings.EqualFold("Name", f):
		case strings.EqualFold("Description", f):
		case strings.EqualFold("Address", f):
			h.Address = strings.TrimSpace(h.Address)
			if len(h.Address) < MinHostAddressLength || len(h.Address) > MaxHostAddressLength {
				return nil, db.NoRowsAffected, errors.New(errors.InvalidAddress, "YyAgOFKnTL")
			}
		default:
			return nil,
				db.NoRowsAffected,
				errors.New(
					errors.InvalidFieldMask,
					"SNgxP6ngNk",
					errors.WithMsg(fmt.Sprintf("invalid field mask: %s", f)),
				)
		}
	}
	var dbMask, nullFields []string
	dbMask, nullFields = dbcommon.BuildUpdatePaths(
		map[string]interface{}{
			"Name":        h.Name,
			"Description": h.Description,
			"Address":     h.Address,
		},
		fieldMaskPaths,
		nil,
	)
	if len(dbMask) == 0 && len(nullFields) == 0 {
		return nil, db.NoRowsAffected, errors.New(errors.EmptyFieldMask, "CcCFJsoFzP")
	}

	oplogWrapper, err := r.kms.GetWrapper(ctx, scopeId, kms.KeyPurposeOplog)
	if err != nil {
		return nil, db.NoRowsAffected, errors.Wrap(err, "kuAQInmfOm", errors.WithMsg("unable to get oplog wrapper"))
	}

	var rowsUpdated int
	var returnedHost *Host
	_, err = r.writer.DoTx(ctx, db.StdRetryCnt, db.ExpBackoff{},
		func(_ db.Reader, w db.Writer) error {
			returnedHost = h.clone()
			var err error
			rowsUpdated, err = w.Update(ctx, returnedHost, dbMask, nullFields,
				db.WithOplog(oplogWrapper, h.oplog(oplog.OpType_OP_TYPE_UPDATE)),
				db.WithVersion(&version))
			if err == nil && rowsUpdated > 1 {
				return errors.New(errors.MultipleRecords, "xJBYJRMXXe")
			}
			return err
		},
	)

	if err != nil {
		if dErr := errors.Convert(err, "dEmKLWCiWN"); dErr != nil {
			return nil, db.NoRowsAffected, dErr
		}
		return nil, db.NoRowsAffected, errors.New(
			errors.Unknown,
			"Hap3HE7q12",
			errors.WithMsg(fmt.Sprintf("catalog: %s: %q", h.CatalogId, h.Name)),
			errors.WithWrap(err),
		)
	}

	return returnedHost, rowsUpdated, nil
}

// LookupHost will look up a host in the repository. If the host is not
// found, it will return nil, nil. All options are ignored.
func (r *Repository) LookupHost(ctx context.Context, publicId string, opt ...Option) (*Host, error) {
	if publicId == "" {
		return nil, errors.New(errors.MissingPublicId, "I4MUUz0Ogf")
	}
	h := allocHost()
	h.PublicId = publicId
	if err := r.reader.LookupByPublicId(ctx, h); err != nil {
		if errors.Is(err, errors.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "Ljwlcf1AdE", errors.WithMsg(fmt.Sprintf("lookup failed for %s", publicId)))
	}
	return h, nil
}

// ListHosts returns a slice of Hosts for the catalogId.
// WithLimit is the only option supported.
func (r *Repository) ListHosts(ctx context.Context, catalogId string, opt ...Option) ([]*Host, error) {
	if catalogId == "" {
		return nil, errors.New(errors.MissingCatalogId, "mJOYA8ZZIF")
	}
	opts := getOpts(opt...)
	limit := r.defaultLimit
	if opts.withLimit != 0 {
		// non-zero signals an override of the default limit for the repo.
		limit = opts.withLimit
	}
	var hosts []*Host
	err := r.reader.SearchWhere(ctx, &hosts, "catalog_id = ?", []interface{}{catalogId}, db.WithLimit(limit))
	if err != nil {
		return nil, errors.Wrap(err, "gvurB0agGz")
	}
	return hosts, nil
}

// DeleteHost deletes the host for the provided id from the repository
// returning a count of the number of records deleted. All options are
// ignored.
func (r *Repository) DeleteHost(ctx context.Context, scopeId string, publicId string, opt ...Option) (int, error) {
	if publicId == "" {
		return db.NoRowsAffected, errors.New(errors.MissingPublicId, "H4PoOJ95Tx")
	}
	h := allocHost()
	h.PublicId = publicId

	oplogWrapper, err := r.kms.GetWrapper(ctx, scopeId, kms.KeyPurposeOplog)
	if err != nil {
		return db.NoRowsAffected, errors.Wrap(err, "EUyTZNkicA", errors.WithMsg("unable to get oplog wrapper"))
	}

	var rowsDeleted int
	_, err = r.writer.DoTx(
		ctx, db.StdRetryCnt, db.ExpBackoff{},
		func(_ db.Reader, w db.Writer) (err error) {
			dh := h.clone()
			rowsDeleted, err = w.Delete(ctx, dh, db.WithOplog(oplogWrapper, h.oplog(oplog.OpType_OP_TYPE_DELETE)))
			if err == nil && rowsDeleted > 1 {
				return errors.New(errors.MultipleRecords, "F6r9DCCqM7")
			}
			return err
		},
	)

	if err != nil {
		return db.NoRowsAffected, errors.Wrap(err, "R7CFLLqqYn", errors.WithMsg(fmt.Sprintf("failed to delete %s", publicId)))
	}

	return rowsDeleted, nil
}
