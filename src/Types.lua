-- ServerPackages/elasticsearchservice/Types.lua
--!strict

export type AnalyticsConfig = {
	elasticsearchUrl: string,
	apiKey: string,
	indexName: string,
	rateLimit: number?,
	maxRetries: number?,
	testConnectionOnInit: boolean?,
}

export type QueuedEvent = {
	document: { [string]: any },
	attempts: number,
}

return {}
