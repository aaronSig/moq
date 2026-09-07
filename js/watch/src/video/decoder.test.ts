import { afterEach, beforeEach, expect, test } from "bun:test";
import * as Catalog from "@moq/hang/catalog";
import { Legacy } from "@moq/hang/container";
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
	config?: VideoDecoderConfig;
	state = "unconfigured";
	readonly init: VideoDecoderInit;
	constructor(init: VideoDecoderInit) {
		this.init = init;
		Codec.instances.push(this);
	}
	configure(config: VideoDecoderConfig) {
		this.config = config;
		this.state = "configured";
	}
	decode(chunk: EncodedVideoChunk) {
		void this.init.output(new Frame(chunk.timestamp) as unknown as VideoFrame);
	}
	close() {
		this.state = "closed";
	}
}
const saved = new Map<string, PropertyDescriptor | undefined>();
beforeEach(() => {
	Codec.instances = [];
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
function fixture() {
	const low = Catalog.VideoConfigSchema.parse({
		codec: "avc1.64001f",
		bitrate: 400_000,
		container: { kind: "legacy" },
	});
	const high = Catalog.VideoConfigSchema.parse({
		codec: "hvc1.1.6.L93.90",
		bitrate: 2_000_000,
		container: { kind: "legacy" },
	});
	const available = new Signal({ low, high });
	const selected = new Signal<string | undefined>("low");
	const config = new Signal<Catalog.VideoConfig | undefined>(low);
	const producers = { low: new Track.Producer("low").accept(), high: new Track.Producer("high").accept() };
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
	let resets = 0;
	const sync = {
		out: { buffer: new Signal(Time.Milli.zero), reference: new Signal(0) },
		received: (pts: number) => received.push(pts),
		reset: () => {
			resets++;
		},
		wait: async () => {},
	} as unknown as Sync;
	const decoder = new Decoder(source, sync, { enabled: true, paced: false });
	function send(name: keyof typeof producers, pts: number) {
		const group = producers[name].appendGroup();
		group.writeFrame({
			payload: Legacy.encodeFrame(new Uint8Array([1]), Time.Micro(pts * 1000)),
			timestamp: Time.Timestamp.fromMicros(Time.Micro(pts * 1000)),
		});
		group.close();
	}
	return {
		decoder,
		selected,
		config,
		available,
		opened,
		received,
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
