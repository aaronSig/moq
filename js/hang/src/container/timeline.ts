/**
 * Publishing a media track's timeline: a companion track that maps each of the track's groups to
 * its start timestamp, so a consumer can seek (or build an HLS/DASH playlist) without downloading
 * the media. See the catalog {@link Catalog.Timeline} section that advertises it.
 *
 * @module
 */

import * as Json from "@moq/json";
import type * as Moq from "@moq/net";
import type { Time } from "@moq/net";
import type * as Catalog from "../catalog";
import { MOQ_EPOCH_UNIX_MILLIS, u53 } from "../catalog";

/** One timeline record: the media track opened `group` at presentation time `pts` (in the timeline's timescale). */
export interface Record {
	group: number;
	pts: number;
}

/** A recent group boundary, expressed in milliseconds of media time. */
export interface Entry {
	group: number;
	ptsMs: number;
}

/** A bounded index for live handoffs. It never extrapolates group numbers between renditions. */
export class Index {
	#entries: Entry[] = [];
	#updatedAt?: number;

	readonly timescale: number;
	constructor(timescale = DEFAULT_TIMESCALE) {
		this.timescale = timescale;
	}

	/** Keep only valid, monotonic records from the current timeline epoch. */
	push(record: Record, now = performance.now()): void {
		if (
			!Number.isSafeInteger(record.group) ||
			record.group < 0 ||
			!Number.isSafeInteger(record.pts) ||
			record.pts < 0 ||
			!Number.isFinite(this.timescale) ||
			this.timescale <= 0
		)
			return;
		const entry = { group: record.group, ptsMs: (record.pts * 1000) / this.timescale };
		if (!Number.isFinite(entry.ptsMs)) return;
		const last = this.#entries.at(-1);
		if (last && (entry.group < last.group || entry.ptsMs < last.ptsMs)) this.#entries = [];
		else if (last && entry.group === last.group) return;
		this.#entries.push(entry);
		// Retain at most 64 records and eight seconds, even for an unbounded timeline log.
		this.#entries = this.#entries.filter((value) => value.ptsMs >= entry.ptsMs - 8000).slice(-64);
		this.#updatedAt = now;
	}

	/** Return a recorded group at/before the target, only while both the index and lookback are fresh. */
	lookup(targetMs: number, maxLookbackMs: number, now = performance.now()): Entry | undefined {
		if (
			!Number.isFinite(targetMs) ||
			!Number.isFinite(maxLookbackMs) ||
			maxLookbackMs <= 0 ||
			this.#updatedAt === undefined ||
			now - this.#updatedAt > 2000 ||
			now < this.#updatedAt
		)
			return;
		const entry = this.#entries.findLast((value) => value.ptsMs <= targetMs);
		if (!entry || targetMs - entry.ptsMs > Math.min(2000, maxLookbackMs)) return;
		return { ...entry };
	}
}

/** Keeps a small live index from the advertised compressed timeline. Owns and closes its subscription. */
export class Consumer {
	readonly index: Index;
	#track: Moq.Track.Subscriber;
	#closed = false;

	constructor(track: Moq.Track.Subscriber, section: Catalog.Timeline) {
		this.#track = track;
		const index = new Index(section.timescale);
		this.index = index;
		const stream = new Json.Stream.Consumer<Record>(track, { compression: true });
		void (async () => {
			try {
				let batch = 0;
				for (;;) {
					const record = await stream.next();
					if (this.#closed) return;
					if (record === undefined) {
						this.close();
						return;
					}
					index.push(record);
					// A long-running publisher's cached metadata can contain thousands of
					// immediately available records. Let rendering run between bounded batches.
					if (++batch === 128) {
						batch = 0;
						await new Promise((resolve) => setTimeout(resolve, 0));
					}
				}
			} catch {
				// Timeline hints must never fail otherwise playable media.
				this.close();
			}
		})();
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#track.close();
	}

	/** Look up only while the timeline is still usable. Media playback can fall back to live delivery. */
	lookup(targetMs: number, maxLookbackMs: number): Entry | undefined {
		return this.#closed ? undefined : this.index.lookup(targetMs, maxLookbackMs);
	}
}

/** The default timeline timescale: 1000 units per second (milliseconds). */
export const DEFAULT_TIMESCALE = 1000;

/** The default record throttle: at most one record per second of media time. */
export const DEFAULT_GRANULARITY_MS = 1000;

/**
 * The conventional companion timeline track name for a media rendition: `<rendition>.timeline.z`
 * (the `.z` marks the DEFLATE-compressed stream, like the catalog's `.json.z` sibling).
 */
export function trackName(rendition: string): string {
	return `${rendition}.timeline.z`;
}

/** Options for a timeline {@link Producer}. */
export interface ProducerProps {
	/** Units per second for the records' `pts` (and the `wall` anchor). Defaults to milliseconds. */
	timescale?: number;

	/**
	 * Record at most one group per this much media time, in milliseconds. Video keyframes are
	 * already this far apart so every group is indexed; short audio groups are thinned out (a
	 * consumer extrapolates or fetches to fill a gap). Defaults to {@link DEFAULT_GRANULARITY_MS}.
	 */
	granularity?: number;
}

/**
 * Publishes one media track's timeline: an NDJSON record per group open, DEFLATE-compressed.
 *
 * {@link record} appends a group's start once. Advertise it in the rendition's catalog config via
 * {@link section}, and attach it to a {@link Legacy.Producer} (its `timeline` prop) to record group
 * opens automatically.
 */
export class Producer {
	#stream: Json.Stream.Producer<Record>;
	#track: string;
	#timescale: number;
	// The wall-clock time of pts 0, in timescale units since the moq epoch (advertised in the section).
	#wall?: number;
	// Minimum media-time gap between recorded groups (throttle), in microseconds.
	#granularityUs: number;
	// The pts (microseconds) of the last recorded group.
	#lastPts?: number;

	/** Wrap an already-created MoQ track (named per {@link trackName}) to publish a rendition's timeline. */
	constructor(track: Moq.Track.Producer, props: ProducerProps = {}) {
		this.#track = track.name;
		this.#timescale = props.timescale ?? DEFAULT_TIMESCALE;
		this.#granularityUs = (props.granularity ?? DEFAULT_GRANULARITY_MS) * 1000;
		this.#stream = new Json.Stream.Producer<Record>(track, { compression: true });
	}

	/** The catalog section advertising this timeline, to attach to the rendition's config. */
	section(): Catalog.Timeline {
		return {
			track: this.#track,
			timescale: u53(this.#timescale),
			wall: this.#wall === undefined ? undefined : u53(this.#wall),
		};
	}

	/**
	 * Set (or replace) the wall-clock anchor advertised in the catalog section, from an observed
	 * pairing of a media timestamp `pts` (microseconds) with its wall-clock time `wall` (defaulting
	 * to now). Stored as the extrapolated wall-clock time of pts 0, the single value the catalog
	 * `wall` field carries: in this timeline's timescale, measured from the moq epoch
	 * ({@link Catalog.MOQ_EPOCH_UNIX_MILLIS}, 2020). Throws if `wall` predates the moq epoch
	 * (unrepresentable).
	 */
	setWall(pts: Time.Micro, wall: Date = new Date()): void {
		const unixMillis = wall.getTime();
		if (unixMillis < MOQ_EPOCH_UNIX_MILLIS) {
			throw new Error(`wall time ${unixMillis} predates the moq epoch ${MOQ_EPOCH_UNIX_MILLIS}`);
		}
		const ptsUnits = Math.floor((pts * this.#timescale) / 1_000_000);
		const moqUnits = Math.floor(((unixMillis - MOQ_EPOCH_UNIX_MILLIS) * this.#timescale) / 1000);
		this.#wall = Math.max(0, moqUnits - ptsUnits);
	}

	/**
	 * Record that group `sequence` opened at presentation time `pts` (microseconds), unless it
	 * falls within the {@link ProducerProps.granularity} of the last recorded group (skipped, so a
	 * consumer extrapolates or fetches to fill the gap).
	 */
	record(sequence: number, pts: Time.Micro): void {
		if (this.#lastPts !== undefined && pts < this.#lastPts + this.#granularityUs) return;
		this.#lastPts = pts;
		this.#stream.append({ group: sequence, pts: Math.floor((pts * this.#timescale) / 1_000_000) });
	}

	/** Finish the timeline track. */
	finish(): void {
		this.#stream.finish();
	}
}
