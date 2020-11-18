package static

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/boundary/internal/db"
	"github.com/hashicorp/boundary/internal/errors"
	"github.com/hashicorp/boundary/internal/kms"
	"github.com/hashicorp/boundary/internal/oplog"
)

// CreateCatalog inserts c into the repository and returns a new
// HostCatalog containing the catalog's PublicId. c is not changed. c must
// contain a valid ScopeID. c must not contain a PublicId. The PublicId is
// generated and assigned by the this method. opt is ignored.
//
// Both c.Name and c.Description are optional. If c.Name is set, it must be
// unique within c.ScopeID.
//
// Both c.CreateTime and c.UpdateTime are ignored.
func (r *Repository) CreateCatalog(ctx context.Context, c *HostCatalog, opt ...Option) (*HostCatalog, error) {
	if c == nil {
		return nil, errors.New(errors.InvalidParameter, "WDqVaWow1I", errors.WithMsg("no static host catalog"))
	}
	if c.HostCatalog == nil {
		return nil, errors.New(errors.InvalidParameter, "bniLSZVXtD", errors.WithMsg("no embedded host catalog"))
	}
	if c.ScopeId == "" {
		return nil, errors.New(errors.MissingScopeId, "9aEGoNhqim")
	}
	if c.PublicId != "" {
		return nil, errors.New(errors.InvalidParameter, "UN8PyOHEzg", errors.WithMsg("public id not empty"))
	}
	c = c.clone()

	opts := getOpts(opt...)

	if opts.withPublicId != "" {
		if !strings.HasPrefix(opts.withPublicId, HostCatalogPrefix+"_") {
			return nil, errors.New(
				errors.InvalidParameter,
				"o8KBnSVBtW",
				errors.WithMsg(
					fmt.Sprintf("passed-in public ID %q has wrong prefix, should be %q",
						opts.withPublicId,
						HostCatalogPrefix),
				))
		}
		c.PublicId = opts.withPublicId
	} else {
		id, err := newHostCatalogId()
		if err != nil {
			return nil, errors.Wrap(err, "FscgDjWH5d")
		}
		c.PublicId = id
	}

	oplogWrapper, err := r.kms.GetWrapper(ctx, c.ScopeId, kms.KeyPurposeOplog)
	if err != nil {
		return nil, errors.Wrap(err, "KeVVbWtJil", errors.WithMsg("unable to get oplog wrapper"))
	}

	metadata := newCatalogMetadata(c, oplog.OpType_OP_TYPE_CREATE)

	var newHostCatalog *HostCatalog
	_, err = r.writer.DoTx(
		ctx,
		db.StdRetryCnt,
		db.ExpBackoff{},
		func(_ db.Reader, w db.Writer) error {
			newHostCatalog = c.clone()
			return w.Create(
				ctx,
				newHostCatalog,
				db.WithOplog(oplogWrapper, metadata),
			)
		},
	)

	if err != nil {
		if dErr := errors.Convert(err, "lpmawQlpci"); dErr != nil {
			return nil, dErr
		}
		return nil, errors.New(
			errors.Unknown,
			"YUNviuuTot",
			errors.WithMsg(fmt.Sprintf("scope: %s", c.ScopeId)),
			errors.WithWrap(err),
		)
	}
	return newHostCatalog, nil
}

// UpdateCatalog updates the repository entry for c.PublicId with the
// values in c for the fields listed in fieldMask. It returns a new
// HostCatalog containing the updated values and a count of the number of
// records updated. c is not changed.
//
// c must contain a valid PublicId. Only c.Name and c.Description can be
// updated. If c.Name is set to a non-empty string, it must be unique
// within c.ScopeID.
//
// An attribute of c will be set to NULL in the database if the attribute
// in c is the zero value and it is included in fieldMask.
func (r *Repository) UpdateCatalog(ctx context.Context, c *HostCatalog, version uint32, fieldMask []string, opt ...Option) (*HostCatalog, int, error) {
	if c == nil {
		return nil, db.NoRowsAffected, errors.New(errors.InvalidParameter, "trTNCCHQZn", errors.WithMsg("no static host catalog"))
	}
	if c.HostCatalog == nil {
		return nil, db.NoRowsAffected, errors.New(errors.InvalidParameter, "IKDocJTAHv", errors.WithMsg("no embedded host catalog"))
	}
	if c.PublicId == "" {
		return nil, db.NoRowsAffected, errors.New(errors.MissingPublicId, "SVcfUXTCbG")
	}
	if c.ScopeId == "" {
		return nil, db.NoRowsAffected, errors.New(errors.MissingScopeId, "KoTPLuElUe")
	}
	if len(fieldMask) == 0 {
		return nil, db.NoRowsAffected, errors.New(errors.EmptyFieldMask, "W5HripLn8X")
	}

	var dbMask, nullFields []string
	for _, f := range fieldMask {
		switch {
		case strings.EqualFold("name", f) && c.Name == "":
			nullFields = append(nullFields, "name")
		case strings.EqualFold("name", f) && c.Name != "":
			dbMask = append(dbMask, "name")
		case strings.EqualFold("description", f) && c.Description == "":
			nullFields = append(nullFields, "description")
		case strings.EqualFold("description", f) && c.Description != "":
			dbMask = append(dbMask, "description")
		default:
			return nil, db.NoRowsAffected, errors.New(
				errors.InvalidFieldMask,
				"4UWfx3RKPW",
				errors.WithMsg(fmt.Sprintf("invalid field mask: %s", f)),
			)
		}
	}

	oplogWrapper, err := r.kms.GetWrapper(ctx, c.ScopeId, kms.KeyPurposeOplog)
	if err != nil {
		return nil, db.NoRowsAffected, errors.Wrap(err, "iHhp6zDi4t", errors.WithMsg("unable to get oplog wrapper"))
	}

	c = c.clone()

	metadata := newCatalogMetadata(c, oplog.OpType_OP_TYPE_UPDATE)

	var rowsUpdated int
	var returnedCatalog *HostCatalog
	_, err = r.writer.DoTx(
		ctx,
		db.StdRetryCnt,
		db.ExpBackoff{},
		func(_ db.Reader, w db.Writer) error {
			returnedCatalog = c.clone()
			var err error
			rowsUpdated, err = w.Update(
				ctx,
				returnedCatalog,
				dbMask,
				nullFields,
				db.WithOplog(oplogWrapper, metadata),
				db.WithVersion(&version),
			)
			if err == nil && rowsUpdated > 1 {
				return errors.New(errors.MultipleRecords, "obC7MjAGtj")
			}
			return err
		},
	)

	if err != nil {
		if dErr := errors.Convert(err, "Ebzq6foarI"); dErr != nil {
			return nil, db.NoRowsAffected, dErr
		}
		return nil, db.NoRowsAffected, errors.New(
			errors.Unknown,
			"2mvrnq45Ap",
			errors.WithMsg(fmt.Sprintf("static host catalog: %s", c.PublicId)),
			errors.WithWrap(err),
		)
	}

	return returnedCatalog, rowsUpdated, nil
}

// LookupCatalog returns the HostCatalog for id. Returns nil, nil if no
// HostCatalog is found for id.
func (r *Repository) LookupCatalog(ctx context.Context, id string, opt ...Option) (*HostCatalog, error) {
	if id == "" {
		return nil, errors.New(errors.MissingPublicId, "8krg5Lnj60")
	}
	c := allocCatalog()
	c.PublicId = id
	if err := r.reader.LookupByPublicId(ctx, c); err != nil {
		if errors.Is(err, errors.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "rKb8JERpza", errors.WithMsg(fmt.Sprintf("lookup failed for %s", id)))
	}
	return c, nil
}

// ListCatalogs returns a slice of HostCatalogs for the scopeId. WithLimit is the only option supported.
func (r *Repository) ListCatalogs(ctx context.Context, scopeId string, opt ...Option) ([]*HostCatalog, error) {
	if scopeId == "" {
		return nil, errors.New(errors.MissingScopeId, "HxZpEs1KFP")
	}
	opts := getOpts(opt...)
	limit := r.defaultLimit
	if opts.withLimit != 0 {
		// non-zero signals an override of the default limit for the repo.
		limit = opts.withLimit
	}
	var hostCatalogs []*HostCatalog
	err := r.reader.SearchWhere(ctx, &hostCatalogs, "scope_id = ?", []interface{}{scopeId}, db.WithLimit(limit))
	if err != nil {
		return nil, errors.Wrap(err, "NUfz9b2Gzf")
	}
	return hostCatalogs, nil
}

// DeleteCatalog deletes id from the repository returning a count of the
// number of records deleted.
func (r *Repository) DeleteCatalog(ctx context.Context, id string, opt ...Option) (int, error) {
	if id == "" {
		return db.NoRowsAffected, errors.New(errors.MissingPublicId, "l25QwC2lJI")
	}

	c := allocCatalog()
	c.PublicId = id
	if err := r.reader.LookupByPublicId(ctx, c); err != nil {
		if errors.Is(err, errors.ErrRecordNotFound) {
			return db.NoRowsAffected, nil
		}
		return db.NoRowsAffected, errors.Wrap(err, "I7EbEefwta", errors.WithMsg(fmt.Sprintf("failed to delete %s", id)))
	}
	if c.ScopeId == "" {
		return db.NoRowsAffected, errors.New(errors.MissingScopeId, "JwKurh9Laa")
	}
	oplogWrapper, err := r.kms.GetWrapper(ctx, c.ScopeId, kms.KeyPurposeOplog)
	if err != nil {
		return db.NoRowsAffected, errors.Wrap(err, "9nrh58aKQe", errors.WithMsg("unable to get oplog wrapper"))
	}

	metadata := newCatalogMetadata(c, oplog.OpType_OP_TYPE_DELETE)

	var rowsDeleted int
	var deleteCatalog *HostCatalog
	_, err = r.writer.DoTx(
		ctx,
		db.StdRetryCnt,
		db.ExpBackoff{},
		func(_ db.Reader, w db.Writer) error {
			deleteCatalog = c.clone()
			var err error
			rowsDeleted, err = w.Delete(
				ctx,
				deleteCatalog,
				db.WithOplog(oplogWrapper, metadata),
			)
			if err == nil && rowsDeleted > 1 {
				return errors.New(errors.MultipleRecords, "QOsAOsa7c1")
			}
			return err
		},
	)

	if err != nil {
		return db.NoRowsAffected, errors.Wrap(err, "sbH5pLPOM3", errors.WithMsg(fmt.Sprintf("failed to delete %s", c.PublicId)))
	}

	return rowsDeleted, nil
}
