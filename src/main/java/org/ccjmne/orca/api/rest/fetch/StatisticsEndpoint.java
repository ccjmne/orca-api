package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;

import java.sql.Date;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.rest.resources.TrainingsStatistics;
import org.ccjmne.orca.api.rest.resources.TrainingsStatistics.TrainingsStatisticsBuilder;
import org.ccjmne.orca.api.utils.APIDateFormat;
import org.ccjmne.orca.api.utils.Constants;
import org.jooq.Record;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

@Path("statistics")
public class StatisticsEndpoint {

  private static final Integer DEFAULT_INTERVAL = Integer.valueOf(1);

  private final ResourcesByKeysCommonEndpoint commonResources;
  // TODO: Should not have any use for these two and should delegate
  // restricted data access mechanics to RestrictedResourcesAccess
  private final ResourcesEndpoint resources;
  private final Restrictions      restrictions;

  @Inject
  public StatisticsEndpoint(
                            final ResourcesEndpoint resources,
                            final ResourcesByKeysCommonEndpoint commonResources,
                            final Restrictions restrictions) {
    this.resources = resources;
    this.commonResources = commonResources;
    this.restrictions = restrictions;
  }

  @SuppressWarnings("unchecked")
  @GET
  @Path("trainings")
  // TODO: rewrite
  // TODO: The entire thing is close to being straight-up irrelevant
  // altogether.
  public Map<Integer, Iterable<TrainingsStatistics>> getTrainingsStats(
                                                                       @QueryParam("from") final String fromStr,
                                                                       @QueryParam("to") final String toStr,
                                                                       @QueryParam("interval") final List<Integer> intervals)
      throws ParseException {
    if (!this.restrictions.canAccessTrainings()) {
      throw new ForbiddenException();
    }

    final List<Record> trainings = this.resources.listSessions();

    final Map<Integer, Set<Integer>> certs = Maps.transformValues(this.commonResources.listTrainingTypes(),
                                                                  trty -> ((Map<Integer, Object>) trty.get("certificates")).keySet());
    final Map<Integer, Iterable<TrainingsStatistics>> res = new HashMap<>();

    for (final Integer interval : intervals) {
      final List<TrainingsStatisticsBuilder> trainingsStatsBuilders = new ArrayList<>();

      StatisticsEndpoint.computeDates(fromStr, toStr, interval).stream().reduce((cur, next) -> {
        trainingsStatsBuilders.add(TrainingsStatistics.builder(certs, Range.<Date> closedOpen(cur, next)));
        return next;
      });

      // [from, from+i[,
      // [from+i, from+2i[,
      // [from+2i, from+3i[,
      // ...
      // [to-i, to] <- closed
      trainingsStatsBuilders.get(trainingsStatsBuilders.size() - 1).closeRange();

      StatisticsEndpoint.populateTrainingsStatsBuilders(
                                                        trainingsStatsBuilders,
                                                        trainings,
                                                        (training) -> training.getValue(TRAININGS.TRNG_DATE),
                                                        (builder, training) -> builder.registerTraining(training));
      res.put(interval, trainingsStatsBuilders.stream().map(TrainingsStatisticsBuilder::build).collect(Collectors.toList()));
    }

    return res;
  }

  private static void populateTrainingsStatsBuilders(
                                                     final List<TrainingsStatisticsBuilder> trainingsStatsBuilders,
                                                     final Iterable<Record> trainings,
                                                     final Function<Record, Date> dateMapper,
                                                     final BiConsumer<TrainingsStatisticsBuilder, Record> populateFunction) {
    final Iterator<TrainingsStatisticsBuilder> iterator = trainingsStatsBuilders.iterator();
    TrainingsStatisticsBuilder next = iterator.next();
    for (final Record training : trainings) {
      if (!training.get(TRAININGS.TRNG_OUTCOME).equals(Constants.TRNG_OUTCOME_COMPLETED)) {
        continue;
      }

      final Date relevantDate = dateMapper.apply(training);
      if (relevantDate.before(next.getBeginning())) {
        continue;
      }

      while (!next.getDateRange().contains(relevantDate) && iterator.hasNext()) {
        next = iterator.next();
      }

      if (next.getDateRange().contains(relevantDate)) {
        populateFunction.accept(next, training);
      }
    }
  }

  public static List<Date> computeDates(final String fromStr, final String toStr, final Integer intervalRaw)
      throws ParseException {
    final Date utmost = (toStr == null) ? new Date(new java.util.Date().getTime()) : APIDateFormat.parseAsSql(toStr);
    if (fromStr == null) {
      return Collections.singletonList(utmost);
    }

    final int interval = (intervalRaw != null ? intervalRaw : DEFAULT_INTERVAL).intValue();
    LocalDate cur = APIDateFormat.parseAsSql(fromStr).toLocalDate();
    if (interval == 0) {
      return ImmutableList.<Date> of(Date.valueOf(cur), utmost);
    }

    final List<Date> res = new ArrayList<>();
    do {
      res.add(Date.valueOf(cur));
      cur = cur.plusMonths(interval);
    } while (cur.isBefore(utmost.toLocalDate()));

    res.add(utmost);
    return res;
  }
}
