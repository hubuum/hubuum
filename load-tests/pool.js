import http from "k6/http";
import { check } from "k6";

const baseUrl = (__ENV.HUBUUM_LOAD_BASE_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const token = __ENV.HUBUUM_LOAD_TOKEN || "";
const paths = (__ENV.HUBUUM_LOAD_PATHS || "/api/v1/collections?limit=25&include_total=false")
  .split("|")
  .map((path) => path.trim())
  .filter(Boolean);

const rate = Number.parseInt(__ENV.HUBUUM_LOAD_RATE || "50", 10);
const preAllocatedVUs = Number.parseInt(__ENV.HUBUUM_LOAD_PREALLOCATED_VUS || "25", 10);
const maxVUs = Number.parseInt(__ENV.HUBUUM_LOAD_MAX_VUS || "200", 10);

export const options = {
  discardResponseBodies: true,
  scenarios: {
    pool_pressure: {
      executor: "constant-arrival-rate",
      rate,
      timeUnit: "1s",
      duration: __ENV.HUBUUM_LOAD_DURATION || "2m",
      preAllocatedVUs,
      maxVUs,
    },
  },
  thresholds: {
    checks: ["rate>0.99"],
    http_req_duration: ["p(95)<1000", "p(99)<2000"],
    http_req_failed: ["rate<0.01"],
  },
};

export function setup() {
  if (!token) {
    throw new Error("HUBUUM_LOAD_TOKEN must contain a test API token");
  }
  if (paths.length === 0 || paths.some((path) => !path.startsWith("/"))) {
    throw new Error("HUBUUM_LOAD_PATHS must contain one or more absolute API paths");
  }
}

export default function () {
  const path = paths[__ITER % paths.length];
  const response = http.get(`${baseUrl}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
    tags: { endpoint: path.split("?", 1)[0] },
    timeout: __ENV.HUBUUM_LOAD_REQUEST_TIMEOUT || "5s",
  });

  check(response, {
    "response is successful": (result) => result.status >= 200 && result.status < 300,
  });
}
