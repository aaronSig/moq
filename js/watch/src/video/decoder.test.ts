import { afterEach, beforeEach, expect, test } from "bun:test";
import * as Catalog from "@moq/hang/catalog";
import { Legacy, Timeline } from "@moq/hang/container";
import { Time, Track } from "@moq/net";
import { Signal } from "@moq/signals";
import type { Broadcast } from "../broadcast";
import type { Sync } from "../sync";
import { Decoder } from "./decoder";
import type { Source } from "./source";

const settle = async () => {
	for (let i = 0; i < 12; i++) await new Promise((r) => setTimeout(r, 0));
};
class Frame {
	timestamp: number;
	constructor(timestamp: number) {
		this.timestamp = timestamp;
	}
	clone() {
		return new Frame(this.timestamp);
	}
	close() {}
}
class Codec {
	static instances: Codec[] = [];
	static failConfigure = false;
	static failDecode = false;
	config?: VideoDecoderConfig;
	state = "unconfigured";
	readonly init: VideoDecoderInit;
	constructor(init: VideoDecoderInit) {
		this.init = init;
		Codec.instances.push(this);
	}
	configure(config: VideoDecoderConfig) {
		if (Codec.failConfigure) throw new Error("rejected configuration");
		this.config = config;
		this.state = "configured";
	}
	decode(chunk: EncodedVideoChunk) {
		if (Codec.failDecode) throw new Error("decode failed");
		void this.init.output(new Frame(chunk.timestamp) as unknown as VideoFrame);
	}
	close() {
		this.state = "closed";
	}
}
const saved = new Map<string, PropertyDescriptor | undefined>();
beforeEach(() => {
	Codec.instances = [];
	Codec.failConfigure = false;
	Codec.failDecode = false;
	for (const [key, value] of Object.entries({
		VideoDecoder: Codec,
		EncodedVideoChunk: class {
			timestamp: number;
			constructor(init: EncodedVideoChunkInit) {
				this.timestamp = init.timestamp;
			}
		},
	})) {
		saved.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
		Object.defineProperty(globalThis, key, { value, configurable: true });
	}
});
afterEach(() => {
	for (const [key, descriptor] of saved) {
		if (descriptor) Object.defineProperty(globalThis, key, descriptor);
		else Reflect.deleteProperty(globalThis, key);
	}
});
function fixture(withTimeline = false, historyEnabled: boolean | undefined = undefined) {
	const low = Catalog.VideoConfigSchema.parse({
		codec: "avc1.64001f",
		bitrate: 400_000,
		container: { kind: "legacy" },
		timeline: withTimeline ? { track: "low.timeline.z", timescale: 1000 } : undefined,
	});
	const high = Catalog.VideoConfigSchema.parse({
		codec: "hvc1.1.6.L93.90",
		bitrate: 2_000_000,
		container: { kind: "legacy" },
	});
	const available = new Signal({ low, high });
	const selected = new Signal<string | undefined>("low");
	const config = new Signal<Catalog.VideoConfig | undefined>(low);
	const producers = {
		low: new Track.Producer("low").accept(),
		high: new Track.Producer("high").accept(),
		"low.timeline.z": new Track.Producer("low.timeline.z").accept(),
	};
	const timeline = withTimeline ? new Timeline.Producer(producers["low.timeline.z"], { granularity: 0 }) : undefined;
	const opened: { name: string; sub: Track.Subscriber; options: Track.Subscription }[] = [];
	const wire = {
		track: (name: keyof typeof producers) => ({
			subscribe: (options: Track.Subscription) => {
				const sub = producers[name].subscribe(options);
				opened.push({ name, sub, options });
				return sub;
			},
		}),
	};
	const broadcast = { relativeBroadcast: () => wire } as unknown as Broadcast;
	const source = {
		in: { broadcast: new Signal(broadcast) },
		out: { track: selected, config, available, catalog: new Signal({ renditions: { low, high } }) },
	} as unknown as Source;
	const received: number[] = [];
	const paced = new Signal(false);
	const waits: (() => void)[] = [];
	let resets = 0;
	const buffer = new Signal(Time.Milli.zero);
	const sync = {
		out: { buffer, reference: new Signal(0) },
		received: (pts: number) => received.push(pts),
		reset: () => {
			resets++;
		},
		wait: () => new Promise<void>((resolve) => waits.push(resolve)),
	} as unknown as Sync;
	const replacementHistory = historyEnabled === undefined ? undefined : new Signal(historyEnabled);
	const decoder = new Decoder(source, sync, { enabled: true, paced, replacementHistory });
	function send(name: keyof typeof producers, pts: number) {
		const group = producers[name].appendGroup();
		group.writeFrame({
			payload: Legacy.encodeFrame(new Uint8Array([1]), Time.Micro(pts * 1000)),
			timestamp: Time.Timestamp.fromMicros(Time.Micro(pts * 1000)),
		});
		group.close();
	}
	return {
		timeline,
		replacementHistory,
		sync,
		buffer,
		decoder,
		selected,
		config,
		available,
		opened,
		received,
		paced,
		release: () => {
			for (const resolve of waits.splice(0)) resolve();
		},
		get resets() {
			return resets;
		},
		send,
		close() {
			decoder.close();
			for (const p of Object.values(producers)) p.close();
		},
	};
}

test("advertised timelines create no demand unless replacement history is enabled", async () => {
	const f = fixture(true);
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		f.selected.set("high");
		await settle();
		f.send("high", 1500);
		await settle();
		f.paced.set(true);
		f.selected.set("low");
		await settle();
		expect(f.decoder.in.replacementHistory.peek()).toBe(false);
		expect(f.opened.filter((value) => value.name === "low.timeline.z")).toHaveLength(0);
		expect(f.opened.filter((value) => value.name === "low").at(-1)?.options.startGroup).toBeUndefined();
	} finally {
		f.close();
		await settle();
	}
});

test("disabling replacement history closes metadata without replacing the playing decoder", async () => {
	const f = fixture(true, true);
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		const media = f.opened.find((value) => value.name === "low");
		const metadata = f.opened.find((value) => value.name === "low.timeline.z");
		expect(metadata).toBeDefined();
		f.replacementHistory?.set(false);
		await settle();
		expect(metadata?.sub.closed.peek()).toBeDefined();
		expect(media?.sub.closed.peek()).toBeUndefined();
		expect(f.opened.filter((value) => value.name === "low")).toHaveLength(1);
		f.replacementHistory?.set(true);
		await settle();
		expect(f.opened.filter((value) => value.name === "low.timeline.z")).toHaveLength(2);
		expect(f.opened.filter((value) => value.name === "low")).toHaveLength(1);
	} finally {
		f.close();
		await settle();
	}
});

test("downshift requests a bounded group from its own warm timeline and keeps retired media closed", async () => {
	const f = fixture(true, true);
	try {
		await settle();
		f.send("low", 1000);
		f.timeline?.record(71, Time.Micro(1000000));
		await settle();
		f.selected.set("high");
		await settle();
		f.send("high", 1500);
		await settle();
		f.timeline?.record(99, Time.Micro(1250000));
		await settle();
		f.buffer.set(Time.Milli(700));
		f.paced.set(true);
		f.selected.set("low");
		await settle();
		const requested = f.opened.filter((value) => value.name === "low").at(-1);
		if (!requested) throw new Error("replacement subscription missing");
		expect(requested.options).toEqual({
			priority: Catalog.PRIORITY.video,
			startGroup: 99,
			ordered: true,
			latencyMax: 700,
		});
		expect(f.decoder.out.receive.peek().pending?.startGroup).toBe(99);
		expect(f.opened.filter((value) => value.name === "low.timeline.z")).toHaveLength(1);
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(1500));
		// A replacement frame older than the outgoing picture must never become visible.
		f.send("low", 1400);
		await settle();
		f.release();
		await settle();
		expect(f.decoder.out.track.peek()).toBe("high");
		f.send("low", 1600);
		await settle();
		f.release();
		await settle();
		expect(f.decoder.out.track.peek()).toBe("low");
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(1600));
		f.decoder.close();
		expect(f.opened.find((value) => value.name === "low.timeline.z")?.sub.closed.peek()).toBeDefined();
	} finally {
		f.close();
		await settle();
	}
});

test("a new track uses its own decoder config before the separate config signal settles", async () => {
	const f = fixture();
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		f.selected.set("high");
		await settle();
		expect(f.opened.map((x) => x.name)).toEqual(["low", "high"]);
		expect(Codec.instances.at(-1)?.config?.codec).toBe("hvc1.1.6.L93.90");
	} finally {
		f.close();
		await settle();
	}
});

test("a pending upgrade cannot move the shared video clock", async () => {
	const f = fixture();
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		f.selected.set("high");
		await settle();
		f.send("high", 900);
		await settle();
		expect(f.received).toEqual([1000]);
		expect(f.decoder.out.track.peek()).toBe("low");
		expect(f.decoder.out.pending.peek()).toBe("high");
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(1000));
	} finally {
		f.close();
		await settle();
	}
});

test("cancelling an upgrade reuses the playing subscription and retires the trial", async () => {
	const f = fixture();
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		f.selected.set("high");
		await settle();
		expect(f.opened[1].options.priority).toBeLessThan(f.opened[0].options.priority ?? 0);
		f.selected.set("low");
		await settle();
		expect(f.opened.map((x) => x.name)).toEqual(["low", "high"]);
		expect(f.opened[1].sub.closed.peek()).not.toBeUndefined();
		expect(f.opened[0].sub.closed.peek()).toBeUndefined();
		expect(f.decoder.out.pending.peek()).toBeUndefined();
		expect(f.decoder.out.track.peek()).toBe("low");
		expect(f.resets).toBe(0);
		f.send("low", 1042);
		await settle();
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(1042));
	} finally {
		f.close();
		await settle();
	}
});

test("a downshift cancels obsolete network demand but keeps its displayed frame", async () => {
	const f = fixture();
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		f.selected.set("high");
		await settle();
		f.send("high", 1042);
		await settle();
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(1042));
		const high = f.opened.find((x) => x.name === "high")!;
		expect(high.sub.subscription.peek()?.priority).toBe(Catalog.PRIORITY.video);
		f.selected.set("low");
		await settle();
		expect(high.sub.closed.peek()).not.toBeUndefined();
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(1042));
		f.send("low", 1084);
		await settle();
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(1084));
		expect(f.resets).toBe(0);
	} finally {
		f.close();
		await settle();
	}
});

test("already decoded old frames drain after a downshift closes their subscription", async () => {
	const f = fixture();
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		f.selected.set("high");
		await settle();
		f.send("high", 1042);
		await settle();
		f.paced.set(true);
		await settle();
		f.send("high", 1084);
		await settle();
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(1042));
		f.selected.set("low");
		await settle();
		expect(f.opened.find((x) => x.name === "high")!.sub.closed.peek()).not.toBeUndefined();
		f.release();
		await settle();
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(1084));
		expect(f.resets).toBe(0);
	} finally {
		f.close();
		await settle();
	}
});

test("the active rendition still owns genuine publisher rewind recovery", async () => {
	const f = fixture();
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		f.send("low", 100);
		await settle();
		expect(f.resets).toBeGreaterThan(0);
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(100));
	} finally {
		f.close();
		await settle();
	}
});

test("runtime decoder failure retires the failed subscription and identifies its codec", async () => {
	const f = fixture();
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		f.selected.set("high");
		await settle();
		Codec.instances.at(-1)!.init.error(new DOMException("Hardware decoder failed", "EncodingError"));
		await settle();
		expect(f.decoder.out.error.peek()).toEqual({
			track: "high",
			codec: "hvc1.1.6.L93.90",
			phase: "decode",
			message: "Hardware decoder failed",
		});
		expect(f.opened[1].sub.closed.peek()).not.toBeUndefined();
		expect(f.opened[0].sub.closed.peek()).toBeUndefined();
		f.selected.set("low");
		await settle();
		f.send("low", 1042);
		await settle();
		expect(f.decoder.out.timestamp.peek()).toBe(Time.Milli(1042));
	} finally {
		f.close();
		await settle();
	}
});

test("configuration and synchronous decode failures are observable", async () => {
	for (const phase of ["configure", "decode"] as const) {
		const f = fixture();
		try {
			await settle();
			f.send("low", 1000);
			await settle();
			if (phase === "configure") Codec.failConfigure = true;
			else Codec.failDecode = true;
			f.selected.set("high");
			await settle();
			if (phase === "decode") {
				f.send("high", 1042);
				await settle();
			}
			expect(f.decoder.out.error.peek()?.phase).toBe(phase);
			expect(f.decoder.out.error.peek()?.track).toBe("high");
			expect(f.opened[1].sub.closed.peek()).not.toBeUndefined();
		} finally {
			Codec.failConfigure = false;
			Codec.failDecode = false;
			f.close();
			await settle();
		}
	}
});

test("late errors from a cancelled trial do not poison the active rendition", async () => {
	const f = fixture();
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		f.selected.set("high");
		await settle();
		const old = Codec.instances.at(-1)!;
		f.selected.set("low");
		await settle();
		old.init.error(new DOMException("Late callback", "EncodingError"));
		await settle();
		expect(f.decoder.out.error.peek()).toBeUndefined();
		expect(f.opened[0].sub.closed.peek()).toBeUndefined();
	} finally {
		f.close();
		await settle();
	}
});

test("pending receive progress is separate from active playback and retires on cancellation", async () => {
	const f = fixture();
	try {
		await settle();
		f.send("low", 1000);
		await settle();
		const active = f.decoder.out.receive.peek().active!;
		expect(active.track).toBe("low");
		expect(active.objectsReceived).toBe(1);
		f.selected.set("high");
		await settle();
		f.send("high", 900);
		await settle();
		const progress = f.decoder.out.receive.peek();
		expect(progress.active?.subscriptionId).toBe(active.subscriptionId);
		expect(progress.pending?.track).toBe("high");
		expect(progress.pending?.objectsReceived).toBe(1);
		expect(progress.pending?.subscriptionId).not.toBe(active.subscriptionId);
		f.selected.set("low");
		await settle();
		expect(f.decoder.out.receive.peek().pending).toBeUndefined();
		expect(f.decoder.out.receive.peek().active?.subscriptionId).toBe(active.subscriptionId);
	} finally {
		f.close();
		await settle();
	}
});
