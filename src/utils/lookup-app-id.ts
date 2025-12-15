import {fetchJson} from './http'

export async function lookupAppId(
  bundleId: string,
  token: string
): Promise<string> {
  const params = new URLSearchParams()
  params.set('filter[bundleId]', bundleId)

  const response = await fetchJson<{
    data?: Array<{id?: string}>
  }>(
    `/apps?${params.toString()}`,
    token,
    'Failed to locate App Store Connect application.'
  )

  const ids = (response.data ?? []).map(app => app.id).filter(Boolean)

  if (ids.length === 0) {
    throw new Error(
      `Unable to find App Store Connect app for bundle id ${bundleId}.`
    )
  }

  if (ids.length > 1) {
    throw new Error(
      `Multiple apps found for bundle id ${bundleId}; please disambiguate.`
    )
  }

  return ids[0] as string
}
