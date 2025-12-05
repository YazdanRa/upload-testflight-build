import {basename} from 'path'
import {statSync, promises as fs} from 'fs'
import {warning, info} from '@actions/core'
import {UploadParams, UploadResult, Uploader} from './types'
import {generateJwt} from '../auth/jwt'
import {buildPlatform, fetchJson} from '../utils/http'
import {pollUntil} from '../utils/poll'
import {extractAppMetadata} from '../utils/appMetadata'
import {lookupAppId} from '../utils/lookup-app-id'

const MAX_PROCESSING_ATTEMPTS = 20
const PROCESSING_DELAY_MS = 30000
const VISIBILITY_ATTEMPTS = 10
const VISIBILITY_DELAY_MS = 10000

export const appstoreApi: Uploader = {
  async upload(params: UploadParams): Promise<UploadResult> {
    info('Starting App Store API upload backend.')
    const token = generateJwt(
      params.issuerId,
      params.apiKeyId,
      params.apiPrivateKey
    )
    const metadata = await extractAppMetadata(params.appPath)
    info(
      `Extracted metadata: bundleId=${metadata.bundleId}, buildNumber=${metadata.buildNumber}, shortVersion=${metadata.shortVersion}`
    )

    const platform = buildPlatform(params.appType)
    const fileName = basename(params.appPath)
    const fileSize = statSync(params.appPath).size

    info(
      `Preparing build upload for platform=${platform}, file=${fileName}, size=${fileSize} bytes`
    )

    const appId = await lookupAppId(metadata.bundleId, token)
    info(`Resolved appId=${appId} for bundleId=${metadata.bundleId}`)

    const appStoreVersionId = await ensureAppStoreVersion(
      {
        appId,
        platform,
        versionString: metadata.shortVersion
      },
      token
    )
    if (appStoreVersionId) {
      info(
        `Ensured appStoreVersion id=${appStoreVersionId} for ${metadata.shortVersion} (${platform}).`
      )
    }

    const preReleaseVersionId = await lookupPreReleaseVersion(
      {
        appId,
        shortVersion: metadata.shortVersion,
        platform
      },
      token
    )
    if (preReleaseVersionId) {
      info(
        `Found existing preReleaseVersion id=${preReleaseVersionId} for ${metadata.shortVersion} (${platform}).`
      )
    } else {
      warning(
        `No preReleaseVersion found for ${metadata.shortVersion} (${platform}); proceeding and relying on App Store Connect to create it during upload.`
      )
    }

    const buildUpload = await createBuildUpload(
      {
        appId,
        platform,
        cfBundleShortVersionString: metadata.shortVersion,
        cfBundleVersion: metadata.buildNumber
      },
      token
    )
    info(`Created build upload id=${buildUpload.id}.`)

    let uploadOperations: UploadOperation[]
    let buildUploadFileId: string | undefined

    if (buildUpload.uploadOperations?.length) {
      uploadOperations = buildUpload.uploadOperations
      info(
        `Received upload operations from build upload (${uploadOperations.length} chunks).`
      )
    } else {
      const file = await createBuildUploadFile(
        {
          buildUploadId: buildUpload.id,
          fileName,
          fileSize
        },
        token
      )
      uploadOperations = file.uploadOperations
      buildUploadFileId = file.id
      info(
        `Created build upload file id=${file.id} with ${uploadOperations.length} chunks.`
      )
    }

    await performUpload(uploadOperations, params.appPath)
    info('Finished uploading build chunks.')

    if (buildUploadFileId) {
      await completeBuildUploadFile(buildUploadFileId, token)
      info('Marked build upload file as uploaded; waiting for processing.')
    } else {
      await completeBuildUpload(buildUpload.id, token)
      info('Marked build upload as complete; waiting for processing.')
    }

    await pollBuildProcessing({
      bundleId: metadata.bundleId,
      buildNumber: metadata.buildNumber,
      platform,
      token
    })

    return {
      backend: 'appstoreApi',
      raw: {buildUploadId: buildUpload.id, buildUploadFileId}
    }
  }
}

type BuildUpload = {
  id: string
  uploadOperations?: UploadOperation[]
}

type AppStoreVersion = {
  id: string
}

type PreReleaseVersion = {
  id: string
}

type UploadOperation = {
  method: string
  url: string
  offset: number
  length: number
  requestHeaders?: Array<{name: string; value: string}>
}

type BuildUploadFile = {
  id: string
  uploadOperations: UploadOperation[]
}

async function createBuildUpload(
  params: {
    appId: string
    platform: string
    cfBundleShortVersionString: string
    cfBundleVersion: string
  },
  token: string
): Promise<BuildUpload> {
  const payload = {
    data: {
      type: 'buildUploads',
      attributes: {
        platform: params.platform,
        cfBundleShortVersionString: params.cfBundleShortVersionString,
        cfBundleVersion: params.cfBundleVersion
      },
      relationships: {
        app: {
          data: {
            type: 'apps',
            id: params.appId
          }
        }
      }
    }
  }

  const response = await fetchJson<{
    data: {
      id: string
      attributes: {
        uploadOperations?: UploadOperation[]
      }
    }
  }>(
    '/buildUploads',
    token,
    'Failed to create App Store build upload.',
    'POST',
    payload
  )

  const uploadOperations = response.data.attributes.uploadOperations
  return {
    id: response.data.id,
    uploadOperations
  }
}

async function ensureAppStoreVersion(
  params: {
    appId: string
    platform: string
    versionString: string
  },
  token: string
): Promise<string | undefined> {
  try {
    const created = await createAppStoreVersion(params, token)
    info(
      `Created appStoreVersion id=${created} for ${params.versionString} (${params.platform}).`
    )
    return created
  } catch (error: unknown) {
    const message = (error as Error)?.message ?? ''
    // If the version already exists or the token cannot list versions, continue without blocking upload.
    if (message.includes('(403)') || message.includes('(409)')) {
      warning(
        `Proceeding without creating appStoreVersion for ${params.versionString} (${params.platform}): ${message}`
      )
      return undefined
    }
    throw error
  }
}

async function createAppStoreVersion(
  params: {appId: string; platform: string; versionString: string},
  token: string
): Promise<string> {
  const payload = {
    data: {
      type: 'appStoreVersions',
      attributes: {
        platform: params.platform,
        versionString: params.versionString
      },
      relationships: {
        app: {
          data: {
            type: 'apps',
            id: params.appId
          }
        }
      }
    }
  }

  const response = await fetchJson<{data: AppStoreVersion}>(
    '/appStoreVersions',
    token,
    'Failed to create App Store version.',
    'POST',
    payload
  )

  if (!response.data?.id) {
    throw new Error('App Store API did not return appStoreVersion id.')
  }

  return response.data.id
}

async function lookupPreReleaseVersion(
  params: {appId: string; shortVersion: string; platform: string},
  token: string
): Promise<string | undefined> {
  const query = new URLSearchParams()
  query.set('filter[app]', params.appId)
  query.set('filter[platform]', params.platform)
  query.set('filter[version]', params.shortVersion)

  const response = await fetchJson<{data?: PreReleaseVersion[]}>(
    `/preReleaseVersions?${query.toString()}`,
    token,
    'Failed to query pre-release versions.'
  )

  return response.data?.[0]?.id
}

async function createBuildUploadFile(
  params: {
    buildUploadId: string
    fileName: string
    fileSize: number
  },
  token: string
): Promise<BuildUploadFile> {
  const payload = {
    data: {
      type: 'buildUploadFiles',
      attributes: {
        fileName: params.fileName,
        fileSize: params.fileSize,
        assetType: 'ASSET',
        uti: 'com.apple.ipa'
      },
      relationships: {
        buildUpload: {
          data: {
            type: 'buildUploads',
            id: params.buildUploadId
          }
        }
      }
    }
  }

  const response = await fetchJson<{
    data: {
      id: string
      attributes: {
        uploadOperations: UploadOperation[]
      }
    }
  }>(
    '/buildUploadFiles',
    token,
    'Failed to create App Store build upload file.',
    'POST',
    payload
  )

  const uploadOperations = response.data.attributes.uploadOperations
  if (!uploadOperations || uploadOperations.length === 0) {
    throw new Error(
      'App Store API returned no upload operations on buildUploadFile.'
    )
  }

  return {
    id: response.data.id,
    uploadOperations
  }
}

async function performUpload(
  uploadOperations: UploadOperation[],
  appPath: string
): Promise<void> {
  const buffer = await fs.readFile(appPath)

  for (const [index, operation] of uploadOperations.entries()) {
    const slice = buffer.subarray(
      operation.offset,
      operation.offset + operation.length
    )

    const headers: Record<string, string> = {}
    if (operation.requestHeaders) {
      for (const header of operation.requestHeaders) {
        headers[header.name] = header.value
      }
    }

    const response = await fetch(operation.url, {
      method: operation.method,
      headers,
      body: slice
    })

    if (!response.ok) {
      const text = await response.text()
      throw new Error(
        `Failed to upload build chunk (status ${response.status}): ${text}`
      )
    }

    info(
      `Uploaded chunk ${index + 1}/${uploadOperations.length} (${slice.length} bytes).`
    )
  }
}

async function completeBuildUpload(
  uploadId: string,
  token: string
): Promise<void> {
  await fetchJson(
    `/buildUploads/${uploadId}/complete`,
    token,
    'Failed to finalize App Store build upload.',
    'POST'
  )
}

async function completeBuildUploadFile(
  buildUploadFileId: string,
  token: string
): Promise<void> {
  const payload = {
    data: {
      id: buildUploadFileId,
      type: 'buildUploadFiles',
      attributes: {
        uploaded: true
      }
    }
  }

  await fetchJson(
    `/buildUploadFiles/${buildUploadFileId}`,
    token,
    'Failed to finalize App Store build upload file.',
    'PATCH',
    payload
  )
}

async function pollBuildProcessing(params: {
  bundleId: string
  buildNumber: string
  platform: string
  token: string
}): Promise<void> {
  await pollUntil(
    () => lookupBuildState(params),
    state => state === 'VALID' || state === 'PROCESSING',
    {
      attempts: VISIBILITY_ATTEMPTS,
      delayMs: VISIBILITY_DELAY_MS,
      onRetry: attempt => {
        warning(
          `Waiting for build ${params.buildNumber} to appear in App Store Connect (attempt ${
            attempt + 1
          }/${VISIBILITY_ATTEMPTS}).`
        )
      }
    }
  )

  await pollUntil(
    () => lookupBuildState(params),
    state => state === 'VALID',
    {
      attempts: MAX_PROCESSING_ATTEMPTS,
      delayMs: PROCESSING_DELAY_MS,
      onRetry: attempt => {
        warning(
          `Build processing pending (attempt ${attempt + 1}/${MAX_PROCESSING_ATTEMPTS}).`
        )
      }
    }
  )

  info('Build upload completed and processing is VALID.')
}

async function lookupBuildState(params: {
  bundleId: string
  buildNumber: string
  platform: string
  token: string
}): Promise<string | undefined> {
  const query = new URLSearchParams()
  query.set('filter[bundleId]', params.bundleId)
  query.set('filter[version]', params.buildNumber)
  query.set('filter[preReleaseVersion.platform]', params.platform)

  const response = await fetchJson<{
    data?: Array<{attributes?: {processingState?: string}}>
  }>(
    `/builds?${query.toString()}`,
    params.token,
    'Failed to query builds for processing state.'
  )

  const state = response.data?.[0]?.attributes?.processingState
  if (state) {
    info(`Build processing state: ${state}`)
  }
  return state
}
