--!strict
-- src/init.lua

local AnalyticsService = require(script.AnalyticsService)
local Types = require(script.Types)

type AnalyticsConfig = Types.AnalyticsConfig
type LogFields = Types.LogFields

return {
	new = AnalyticsService.new,

	-- optional: expose the class table too (if you want)
	AnalyticsService = AnalyticsService,
}
