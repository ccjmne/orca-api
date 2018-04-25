package org.ccjmne.orca.api.modules;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.ccjmne.orca.api.utils.ResourcesHelper;
import org.ccjmne.orca.jooq.classes.tables.records.CertificatesRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectQuery;

/**
 * Concentrate all accesses to the database (all usages of a {@link DSLContext})
 * that are not meant to be filtered according to the {@link Restrictions}
 * module.
 */
public class ResourcesUnrestricted {

	private static final Field<String[]> TAG_VALUES = ResourcesHelper
			.arrayAggDistinctOmitNull(SITES_TAGS.SITA_VALUE).as("values");

	// TODO: make private when rewriting training statistics computing
	public static final Field<Integer[]> TRAININGTYPE_CERTIFICATES = ResourcesHelper
			.arrayAggDistinctOmitNull(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK).as("certificates");

	private final DSLContext ctx;

	@Inject
	public ResourcesUnrestricted(final DSLContext ctx) {
		this.ctx = ctx;
	}

	public List<Map<String, Object>> listTrainingTypes() {
		return this.ctx.select(TRAININGTYPES.fields())
				.select(ResourcesHelper.arrayAgg(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK),
						ResourcesHelper.arrayAgg(TRAININGTYPES_CERTIFICATES.TTCE_DURATION))
				.from(TRAININGTYPES)
				.join(TRAININGTYPES_CERTIFICATES, JoinType.LEFT_OUTER_JOIN).on(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK.eq(TRAININGTYPES.TRTY_PK))
				.groupBy(TRAININGTYPES.fields())
				.orderBy(TRAININGTYPES.TRTY_ORDER)
				.fetch(ResourcesHelper
						.getMapperWithZip(	ResourcesHelper.getZipMapper(false,
																		TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
																		TRAININGTYPES_CERTIFICATES.TTCE_DURATION),
											"certificates"));
	}

	public Result<CertificatesRecord> listCertificates() {
		return this.ctx.selectFrom(CERTIFICATES).orderBy(CERTIFICATES.CERT_ORDER).fetch();
	}

	public List<Map<String, Object>> listTags(final Integer type) {
		try (final SelectQuery<Record> query = this.ctx
				.select(TAGS.fields()).select(TAG_VALUES)
				.from(TAGS).leftOuterJoin(SITES_TAGS).on(SITES_TAGS.SITA_TAGS_FK.eq(TAGS.TAGS_PK))
				.groupBy(TAGS.fields()).getQuery()) {

			if (type != null) {
				query.addConditions(TAGS.TAGS_PK.eq(type));
			}

			return query.fetch(record -> {
				final Map<String, Object> res = new HashMap<>();
				Arrays.asList(TAGS.fields()).forEach(field -> res.put(field.getName(), record.get(field)));
				res.put(TAG_VALUES.getName(),
						Arrays.asList(record.get(TAG_VALUES)).stream()
								.map(value -> ResourcesHelper.coerceTagValue(record.get(TAGS.TAGS_TYPE), value))
								.collect(Collectors.toList()));
				return res;
			});
		}
	}
}
