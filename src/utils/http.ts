import {info} from '@actions/core'

const BASE_URL = 'https://api.appstoreconnect.apple.com/v1'
const RETRY_STATUS_CODES = new Set([429, 500, 502, 503, 504])
const DEFAULT_RETRY: RetryOptions = {retries: 5, baseDelayMs: 1000, factor: 2}

type RetryOptions = {
  retries: number
  baseDelayMs: number
  factor: number
}

export async function fetchJson<T = unknown>(
  path: string,
  token: string,
  errorMessage: string,
  method: 'GET' | 'POST' | 'PATCH' = 'GET',
  body?: unknown,
  extraHeaders?: Record<string, string>,
  retryOptions: RetryOptions = DEFAULT_RETRY
): Promise<T> {
  const normalizedPath = path.startsWith('/') ? path.slice(1) : path
  const url = new URL(normalizedPath, `${BASE_URL}/`)
  const headers = {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
    ...extraHeaders
  }

  const safeHeaders = {
    ...headers,
    Authorization: headers.Authorization ? '[REDACTED]' : undefined
  }

  const stringifiedBody = body ? JSON.stringify(body) : undefined
  let attempt = 0
  let lastError: Error | undefined

  while (attempt <= retryOptions.retries) {
    const attemptStart = Date.now()
    info(
      `HTTP request: ${method} ${url.toString()} headers=${JSON.stringify(
        safeHeaders
      )} body=${stringifiedBody ?? '<none>'} attempt=${attempt + 1}/${
        retryOptions.retries + 1
      }`
    )

    let response: Response
    try {
      response = await fetch(url, {
        method,
        headers,
        body: stringifiedBody
      })
    } catch (error: unknown) {
      lastError =
        error instanceof Error
          ? error
          : new Error(`${errorMessage}: ${String(error)}`)

      if (attempt === retryOptions.retries) {
        break
      }

      const backoff =
        retryOptions.baseDelayMs * Math.pow(retryOptions.factor, attempt)
      info(
        `Retrying ${method} ${url.toString()} after ${backoff}ms (attempt ${
          attempt + 1
        }) fetch-error=${lastError.message}`
      )
      await delay(backoff)
      attempt += 1
      continue
    }

    const responseText = await response.text()
    const durationMs = Date.now() - attemptStart
    const requestId =
      response.headers.get('x-request-id') ??
      response.headers.get('x-apple-request-id') ??
      response.headers.get('request-id')
    const responseHeaders = headersToObject(response.headers)
    info(
      `HTTP response: ${method} ${url.toString()} status=${response.status} ${response.statusText} duration=${durationMs}ms request-id=${
        requestId ?? 'n/a'
      } headers=${JSON.stringify(responseHeaders)} body=${responseText}`
    )

    if (response.ok) {
      if (response.status === 204) {
        return {} as T
      }

      const contentType = response.headers.get('content-type')
      if (!contentType || !contentType.includes('application/json')) {
        return {} as T
      }

      return JSON.parse(responseText) as T
    }

    lastError = new Error(
      `${errorMessage} (${response.status}): ${responseText}`
    )

    if (!RETRY_STATUS_CODES.has(response.status)) {
      break
    }

    if (attempt === retryOptions.retries) {
      break
    }

    const backoff =
      retryOptions.baseDelayMs * Math.pow(retryOptions.factor, attempt)
    info(
      `Retrying ${method} ${url.toString()} after ${backoff}ms (attempt ${
        attempt + 1
      }) lastStatus=${response.status} request-id=${requestId ?? 'n/a'}`
    )
    await delay(backoff)
    attempt += 1
  }

  throw (
    lastError ?? new Error(`${errorMessage}: request failed without response`)
  )
}

export function buildPlatform(appType: string): string {
  switch (appType.toLowerCase()) {
    case 'macos':
      return 'MAC_OS'
    case 'appletvos':
      return 'TV_OS'
    case 'visionos':
      return 'VISION_OS'
    default:
      return 'IOS'
  }
}

async function delay(durationMs: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, durationMs)
  })
}

function headersToObject(headers: Headers): Record<string, string> {
  const result: Record<string, string> = {}
  for (const [key, value] of headers.entries()) {
    result[key] = key.toLowerCase() === 'authorization' ? '[REDACTED]' : value
  }
  return result
}
