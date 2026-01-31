--!strict
-- AnalyticsService.lua
-- A reusable analytics service for Roblox with Elasticsearch integration

local HttpService = game:GetService("HttpService")

local Types = require(script.Parent.Types)

type AnalyticsConfig = Types.AnalyticsConfig
type LogFields = Types.LogFields
type QueuedEvent = Types.QueuedEvent

local AnalyticsService = {}
AnalyticsService.__index = AnalyticsService

-- Default configuration values
local DEFAULT_RATE_LIMIT = 0.5
local DEFAULT_MAX_RETRIES = 3
local DEFAULT_INDEX_PREFIX = "roblox"
local DEFAULT_TEST_CONNECTION = true

--[=[
	Creates a new AnalyticsService instance.
	
	@param config AnalyticsConfig -- Configuration table with required elasticsearchUrl and apiKey
	@return AnalyticsService
]=]
function AnalyticsService.new(config: AnalyticsConfig)
	assert(config, "AnalyticsService requires a config table")
	assert(config.elasticsearchUrl, "AnalyticsService requires 'elasticsearchUrl' in config")
	assert(config.apiKey, "AnalyticsService requires 'apiKey' in config")

	local self = setmetatable({}, AnalyticsService)

	-- Store configuration
	self._elasticsearchUrl = config.elasticsearchUrl
	self._apiKey = config.apiKey
	self._rateLimit = config.rateLimit or DEFAULT_RATE_LIMIT
	self._maxRetries = config.maxRetries or DEFAULT_MAX_RETRIES
	self._placeNames = config.placeNames or {}
	self._indexPrefix = config.indexPrefix or DEFAULT_INDEX_PREFIX
	self._testConnectionOnInit = if config.testConnectionOnInit ~= nil
		then config.testConnectionOnInit
		else DEFAULT_TEST_CONNECTION

	-- Internal state
	self._requestQueue = {} :: { QueuedEvent }
	self._lastRequestTime = 0
	self._isRunning = false

	return self
end

--[=[
	Initializes the service, starting the queue processor and optionally testing connection.
	Call this after creating the service instance.
]=]
function AnalyticsService:Init()
	if self._isRunning then
		warn("[AnalyticsService] Already initialized")
		return
	end

	self._isRunning = true

	task.spawn(function()
		self:_processQueue()
	end)

	game:BindToClose(function()
		local deadline = tick() + 25
		while #self._requestQueue > 0 and tick() < deadline do
			task.wait(0.1)
		end
	end)

	if self._testConnectionOnInit then
		self:_testConnection()
	end
end

--[=[
	Stops the queue processor gracefully.
]=]
function AnalyticsService:Stop()
	self._isRunning = false
end

function AnalyticsService:_testConnection()
	local success, result = pcall(function()
		return HttpService:RequestAsync({
			Url = self._elasticsearchUrl .. "/_cluster/health",
			Method = "GET",
			Headers = {
				["Authorization"] = "ApiKey " .. self._apiKey,
			},
		})
	end)

	if success and result.Success then
		print("[AnalyticsService] Connected to Elasticsearch")
	else
		warn("[AnalyticsService] Failed to connect to Elasticsearch")
		if result then
			warn("  Status:", result.StatusCode)
			warn("  Body:", result.Body)
		else
			warn("  Error:", tostring(success))
		end
	end
end

--[=[
	Logs an analytics event to Elasticsearch.
	
	@param category string -- The event category (used in index name)
	@param fields LogFields -- Key-value pairs of data to log
	@param player Player? -- Optional player to auto-inject context from
]=]
function AnalyticsService:Log(category: string, fields: LogFields, player: Player?)
	if not category or not fields then
		warn("[AnalyticsService] Missing category or fields")
		return
	end

	-- Create a copy to avoid mutating the original
	local eventFields = table.clone(fields)

	-- Auto-inject player context if provided
	if player and player:IsA("Player") then
		local playerGuid = player:GetAttribute("playerGuid")

		if playerGuid then
			eventFields.playerGuid = playerGuid
		else
			warn("[AnalyticsService] Player " .. player.Name .. " has no playerGuid attribute!")
		end

		eventFields.userId = player.UserId
		eventFields.username = player.Name
	end

	-- Add timestamp and place context
	eventFields.timestamp = DateTime.now():ToIsoDate()
	eventFields.placeId = self._placeNames[game.PlaceId] or tostring(game.PlaceId)
	eventFields.jobId = game.JobId

	table.insert(self._requestQueue, {
		category = category,
		fields = eventFields,
		attempts = 0,
	})
end

function AnalyticsService:_processQueue()
	while self._isRunning do
		task.wait()

		if tick() - self._lastRequestTime < self._rateLimit then
			continue
		end

		local event = self._requestQueue[1]
		if not event then
			continue
		end

		local success = self:_sendEvent(event.category, event.fields)

		if success then
			table.remove(self._requestQueue, 1)
			self._lastRequestTime = tick()
		else
			event.attempts = event.attempts + 1
			if event.attempts >= self._maxRetries then
				warn("[AnalyticsService] Dropping event after " .. self._maxRetries .. " failures: " .. event.category)
				table.remove(self._requestQueue, 1)
			end
		end
	end
end

function AnalyticsService:_sendEvent(category: string, fields: LogFields): boolean
	-- Create index name with year
	local indexName = string.format("%s-%s-%s", self._indexPrefix, category, os.date("%Y"))

	-- Elasticsearch bulk API format
	local bulkCommand = string.format('{"create":{"_index":"%s"}}', indexName)
	local body = bulkCommand .. "\n" .. HttpService:JSONEncode(fields) .. "\n"

	local success, result = pcall(function()
		return HttpService:RequestAsync({
			Url = self._elasticsearchUrl .. "/_bulk",
			Method = "POST",
			Headers = {
				["Content-Type"] = "application/x-ndjson",
				["Authorization"] = "ApiKey " .. self._apiKey,
			},
			Body = body,
		})
	end)

	if not success then
		warn("[AnalyticsService] Request error:", result)
		return false
	end

	if not result.Success then
		warn("[AnalyticsService] Request failed:", result.StatusCode)
		warn("Response:", result.Body)
		return false
	end

	-- Check if bulk operation had errors
	local decodeSuccess, response = pcall(function()
		return HttpService:JSONDecode(result.Body)
	end)

	if decodeSuccess and response.errors then
		warn("[AnalyticsService] Bulk operation had errors")
		return false
	end

	return true
end

--[=[
	Returns the current queue length (useful for debugging/monitoring).
	@return number
]=]
function AnalyticsService:GetQueueLength(): number
	return #self._requestQueue
end

return AnalyticsService
