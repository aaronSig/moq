import { expect, test } from "bun:test";
import { Time, Track } from "@moq/net";
import { Consumer, Index, Producer } from "./timeline";

test("timeline lookup uses the recorded target group's own timescale and never predicts a group", () => {
	const index = new Index(90000);
	index.push({ group: 71, pts: 90000 }, 0);
	index.push({ group: 99, pts: 180000 }, 1);
	expect(index.lookup(999, 2000, 2)).toBeUndefined();
	expect(index.lookup(1000, 2000, 2)).toEqual({ group: 71, ptsMs: 1000 });
	expect(index.lookup(1999, 2000, 2)).toEqual({ group: 71, ptsMs: 1000 });
	expect(index.lookup(2000, 2000, 2)).toEqual({ group: 99, ptsMs: 2000 });
	expect(index.lookup(2200, 2000, 2)).toEqual({ group: 99, ptsMs: 2000 });
});

test("missing, stale and distant timeline hints fall back instead of requesting old media", () => {
	const index = new Index();
	expect(index.lookup(1000, 700, 0)).toBeUndefined();
	index.push({ group: 500, pts: 1000 }, 10);
	expect(index.lookup(1701, 700, 11)).toBeUndefined();
	expect(index.lookup(1000, 700, 2011)).toBeUndefined();
	expect(index.lookup(4000, 10000, 11)).toBeUndefined();
	expect(index.lookup(NaN, 700, 11)).toBeUndefined();
});

test("timeline cache evicts old records and clears the previous epoch on rewind", () => {
	const index = new Index();
	for (let n = 0; n < 1000; n++) index.push({ group: n * 20, pts: n * 10 }, n);
	expect(index.lookup(9000, 2000, 1000)).toBeUndefined();
	expect(index.lookup(9990, 2000, 1000)?.group).toBe(19980);
	index.push({ group: 20000, pts: 0 }, 1001);
	expect(index.lookup(9990, 2000, 1002)).toBeUndefined();
	expect(index.lookup(0, 2000, 1002)?.group).toBe(20000);
	index.push({ group: -1, pts: 1 }, 1003);
	index.push({ group: 20001, pts: NaN }, 1003);
	expect(index.lookup(0, 2000, 1004)?.group).toBe(20000);
});

test("compressed live timeline is usable while owned and closes when cancelled", async () => {
	const track = new Track.Producer("floor.timeline.z").accept();
	const producer = new Producer(track, { granularity: 0 });
	const sub = track.subscribe();
	const consumer = new Consumer(sub, producer.section());
	producer.record(400, Time.Micro(1200000));
	producer.record(420, Time.Micro(1450000));
	for (let n = 0; n < 8; n++) await new Promise((resolve) => setTimeout(resolve, 0));
	expect(consumer.lookup(1500, 700)).toEqual({ group: 420, ptsMs: 1450 });
	consumer.close();
	expect(consumer.lookup(1500, 700)).toBeUndefined();
	expect(sub.closed.peek()).toBeDefined();
	producer.finish();
	track.close();
});
