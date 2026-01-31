--!strict
-- Types.lua
-- Type definitions for AnalyticsService

export type AnalyticsConfig = {
	elasticsearchUrl: string,
	apiKey: string,
	rateLimit: number?,
	maxRetries: number?,
	placeNames: { [number]: string }?,
	indexPrefix: string?,
	testConnectionOnInit: boolean?,
}

export type LogFields = {
	[string]: any,
}

export type QueuedEvent = {
	category: string,
	fields: LogFields,
	attempts: number,
}

return {}
