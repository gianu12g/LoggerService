-- ServerPackages/elasticsearchservice/AnalyticsService.lua
--!strict

local HttpService = game:GetService("HttpService")

local Types = require(script.Parent.Types)

type AnalyticsConfig = Types.AnalyticsConfig
type QueuedEvent = Types.QueuedEvent

local AnalyticsService = {}
AnalyticsService.__index = AnalyticsService

local DEFAULT_RATE_LIMIT = 0.5
local DEFAULT_MAX_RETRIES = 3
local DEFAULT_TEST_CONNECTION = true

function AnalyticsService.new(config: AnalyticsConfig)
	assert(config, "AnalyticsService requires a config table")
	assert(config.elasticsearchUrl, "AnalyticsService requires 'elasticsearchUrl' in config")
	assert(config.apiKey, "AnalyticsService requires 'apiKey' in config")
	assert(config.indexName, "AnalyticsService requires 'indexName' in config")

	local self = setmetatable({}, AnalyticsService)

	self._elasticsearchUrl = config.elasticsearchUrl
	self._apiKey = config.apiKey
	self._indexName = config.indexName

	self._rateLimit = config.rateLimit or DEFAULT_RATE_LIMIT
	self._maxRetries = config.maxRetries or DEFAULT_MAX_RETRIES
	self._testConnectionOnInit = if config.testConnectionOnInit ~= nil
		then config.testConnectionOnInit
		else DEFAULT_TEST_CONNECTION

	self._requestQueue = {} :: { QueuedEvent }
	self._lastRequestTime = 0
	self._isRunning = false

	return self
end

local function _authHeaderValue(apiKey)
	if apiKey == nil then
		return nil
	end

	local t = typeof(apiKey)

	-- ✅ Secret support
	if t == "Secret" then
		return apiKey:AddPrefix("ApiKey ")
	end

	-- ✅ Keep current string support
	if type(apiKey) == "string" then
		-- keep existing behavior if caller already included prefix
		if apiKey:match("^%s*ApiKey%s+") or apiKey:match("^%s*Bearer%s+") then
			return apiKey
		end
		return "ApiKey " .. apiKey
	end

	error(("[elasticsearchservice] apiKey must be string or Secret, got %s"):format(t))
end

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

function AnalyticsService:Stop()
	self._isRunning = false
end

function AnalyticsService:_testConnection()
	local success, result = pcall(function()
		return HttpService:RequestAsync({
			Url = self._elasticsearchUrl .. "/_cluster/health",
			Method = "GET",
			Headers = {
				["Authorization"] = _authHeaderValue(self._apiKey),
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

function AnalyticsService:Log(document: { [string]: any })
	if not document then
		warn("[AnalyticsService] Missing document")
		return
	end

	table.insert(self._requestQueue, {
		document = document,
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

		local success = self:_sendEvent(event.document)

		if success then
			table.remove(self._requestQueue, 1)
			self._lastRequestTime = tick()
		else
			event.attempts = event.attempts + 1
			if event.attempts >= self._maxRetries then
				warn("[AnalyticsService] Dropping event after " .. self._maxRetries .. " failures")
				table.remove(self._requestQueue, 1)
			end
		end
	end
end

function AnalyticsService:_sendEvent(document: { [string]: any }): boolean
	local bulkCommand = string.format('{"create":{"_index":"%s"}}', self._indexName)
	local body = bulkCommand .. "\n" .. HttpService:JSONEncode(document) .. "\n"

	local success, result = pcall(function()
		return HttpService:RequestAsync({
			Url = self._elasticsearchUrl .. "/_bulk",
			Method = "POST",
			Headers = {
				["Content-Type"] = "application/x-ndjson",
				["Authorization"] = _authHeaderValue(self._apiKey),
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

	local decodeSuccess, response = pcall(function()
		return HttpService:JSONDecode(result.Body)
	end)

	if decodeSuccess and response.errors then
		warn("[AnalyticsService] Bulk operation had errors")
		return false
	end

	return true
end

function AnalyticsService:GetQueueLength(): number
	return #self._requestQueue
end

return AnalyticsService
