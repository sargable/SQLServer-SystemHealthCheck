CREATE procedure [dbo].[sp_SystemHealthCheck]
	@system_health_session_file varchar(512) = null,
	@always_on_session_file varchar(512) = null,
	@object_name nvarchar(128) = 'sp_server_diagnostics_component_result',
	@date_from datetime = '1900-01-01',
	@date_to datetime = '2099-12-31',
	@include_xml_columns bit = 1,
	@store_in_database varchar(255) = null,
	@retention_days int = 30
as
/*
MIT License

Copyright (c) 2022 Sargable Group Ltd https://sargable.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

declare @system_health_session_path varchar(512),
		@always_on_session_path varchar(512),
		@system_health_session_basepath varchar(512),
		@always_on_session_basepath varchar(512),
		@system_health_session_address varbinary(8),
		@always_on_session_address varbinary(8),
		@sql varchar(max)

/*
	all objects:
	'wait_info'
	,'sp_server_diagnostics_component_result'
	,'wait_info_external'
	,'memory_broker_ring_buffer_recorded'
	,'error_reported'
	,'scheduler_monitor_system_health_ring_buffer_recorded'
	,'connectivity_ring_buffer_recorded'
*/

--Print 'Get session address'
select @system_health_session_address = [address]
from sys.dm_xe_sessions 
where [name] = 'system_health'

select @always_on_session_address = [address]
from sys.dm_xe_sessions 
where [name] = 'AlwaysOn_health'

if @system_health_session_file is null
	begin
		set @system_health_session_file = 'Current'
	end

if @always_on_session_file is null
	begin
		set @always_on_session_file = 'Current'
	end

set @sql = null;
			
/* Build Target table */
set @sql = case when @store_in_database is not null then '
if object_id(''' + @store_in_database + '.dbo.SystemHealthCheck_Diagnostics'') is null
	begin
		CREATE TABLE [' + @store_in_database + '].[dbo].[SystemHealthCheck_Diagnostics](
			[Sql Instance] varchar(50) not null,
			[Event Time] datetime2(0) not NULL,
			[Max Workers] [bigint] NULL,
			[Pending Tasks] [bigint] NULL,
			[Workers Created] [bigint] NULL,
			[Workers Idle] [bigint] NULL,
			[Sql Cpu Utilization] [bigint] NULL,
			[Os Cpu Utilization] [bigint] NULL,
			[Scheduler Monitor Events] xml,
			[Bad Pages Detected] [int] NULL,
			[Bad Pages Fixed] [int] NULL,
			[Out Of Memory Exceptions] [bigint] NULL,
			[Process Out Of Memory Period] [bigint] NULL,
			[IO Latch Timeouts] [bigint] NULL,
			[Interval Long IOs] [bigint] NULL,
			[Total Long IOs] [bigint] NULL,
			[Available Physical Memory] [bigint] NULL,
			[Available Virtual Memory] [bigint] NULL,
			[Available Paging File] [bigint] NULL,
			[Working Set] [bigint] NULL,
			[Percent of Committed Memory in WS] [bigint] NULL,
			[Page Faults] [bigint] NULL,
			[System physical memory high] [bigint] NULL,
			[System physical memory low] [bigint] NULL,
			[Process physical memory low] [bigint] NULL,
			[Process virtual memory low] [bigint] NULL,
			[VM Reserved] [bigint] NULL,
			[VM Committed] [bigint] NULL,
			[Locked Pages Allocated] [bigint] NULL,
			[Large Pages Allocated] [bigint] NULL,
			[Emergency Memory] [bigint] NULL,
			[Emergency Memory In Use] [bigint] NULL,
			[Target Committed] [bigint] NULL,
			[Current Committed] [bigint] NULL,
			[Pages Allocated] [bigint] NULL,
			[Pages Reserved] [bigint] NULL,
			[Pages Free] [bigint] NULL,
			[Pages In Use] [bigint] NULL,
			[Page Alloc Potential] [bigint] NULL,
			[NUMA Growth Phase] [bigint] NULL,
			[Last OOM Factor] [bigint] NULL,
			[Last OS Error] [bigint] NULL,
			[Long Pending Requests Count] [int] NOT NULL,
			[Long Pending Requests Total Duration] [bigint] NOT NULL,
			[Long Pending Requests] [xml] NULL,
			[Average Wait Time] [bigint] NOT NULL,
			[Average Wait Time Delta] [bigint] NOT NULL,
			[Max Wait Time] [bigint] NOT NULL,
			[Max Wait Time Delta] [bigint] NOT NULL,
			[Top Waits List] [xml] NULL,
			[Intensive Queries Count] [int] NOT NULL,
			[Intensive Queries Total Cpu Utilisation] [bigint] NOT NULL,
			[Intensive Queries Total Cpu ms] [bigint] NOT NULL,
			[CPU Intensive Requests List] [xml] NULL,
			[Pending Tasks List] [xml] NULL,
			[Blocking Tasks List] [int] NOT NULL,
			[Blocked Process Report] [xml] NULL,
			[Event Hour] tinyint,
			constraint pk_SystemHealthCheck_Diagnostics primary key clustered ([Sql Instance], [Event Time])
		) 
	end
' end

if @sql is not null
	begin
		begin try
			exec (@sql)
			set @sql = null
		end try
		begin catch
			select @sql
			throw
		end catch
	end;

/* Set AlwaysOn Health File path */
if @always_on_session_file = 'Current'
	begin
		select @always_on_session_path = cast( t.target_data as xml ).value('(EventFileTarget/File/@name)[1]', 'varchar(max)')
		from sys.dm_xe_session_targets t 
		where t.target_name = 'event_file'
		and [event_session_address] = @always_on_session_address
	end

else if @always_on_session_file = '*' or ISNUMERIC(@always_on_session_file) = 1
	begin
		select @always_on_session_basepath = left(cast( t.target_data as xml ).value('(EventFileTarget/File/@name)[1]', 'varchar(max)'),PATINDEX('%AlwaysOn_health%',cast( t.target_data as xml ).value('(EventFileTarget/File/@name)[1]', 'varchar(max)'))-1)
		from sys.dm_xe_session_targets t 
		where t.target_name = 'event_file'
		and [event_session_address] = @always_on_session_address

		if ISNUMERIC(@always_on_session_file) = 1
			begin
				create table #dirtree1 (
					   id int identity(1,1)
					  ,subdirectory nvarchar(512)
					  ,depth int
					  ,isfile bit);

				insert into #dirtree1
				exec xp_dirtree @always_on_session_basepath, 2, 1

				delete from #dirtree1
				where subdirectory not like 'AlwaysOn_health%'
				
				select @always_on_session_path = @always_on_session_basepath + subdirectory
				from (
					select subdirectory, rn=row_number() over (order by subdirectory desc)
					from #dirtree1
				) t
				where rn = @always_on_session_file

			end
		else
			begin
				set @always_on_session_path = @always_on_session_basepath + 'AlwaysOn_health*.xel'
			end
	end
else
	begin
		declare @always_on_session_fileExists as int = 0;

		exec master.dbo.xp_fileexist @system_health_session_file, @always_on_session_fileExists output;
		if ( @always_on_session_fileExists = 1 )
			begin
				set @always_on_session_path = @always_on_session_file
			end
		else
			begin
				raiserror ('Always On Session File does not exist',16,1)
			end
	end;

/* Set System Health File path */
if @system_health_session_file = 'Current'
	begin
		select @system_health_session_path = cast( t.target_data as xml ).value('(EventFileTarget/File/@name)[1]', 'varchar(max)')
		from sys.dm_xe_session_targets t 
		where t.target_name = 'event_file'
		and [event_session_address] = @system_health_session_address
	end
else if @system_health_session_file = '*' or ISNUMERIC(@system_health_session_file) = 1
	begin

		select @system_health_session_basepath = left(cast( t.target_data as xml ).value('(EventFileTarget/File/@name)[1]', 'varchar(max)'),PATINDEX('%system_health%',cast( t.target_data as xml ).value('(EventFileTarget/File/@name)[1]', 'varchar(max)'))-1)
		from sys.dm_xe_session_targets t 
		where t.target_name = 'event_file'
		and [event_session_address] = @system_health_session_address

		if ISNUMERIC(@system_health_session_file) = 1
			begin
				create table #dirtree2 (
					   id int identity(1,1)
					  ,subdirectory nvarchar(512)
					  ,depth int
					  ,isfile bit);

				insert into #dirtree2
				exec xp_dirtree @system_health_session_basepath, 2, 1

				delete from #dirtree2
				where subdirectory not like 'system_health%'
				
				select @system_health_session_path = @system_health_session_basepath + subdirectory
				from (
					select subdirectory, rn=row_number() over (order by subdirectory desc)
					from #dirtree2
				) t
				where rn = @system_health_session_file

			end
		else
			begin
				set @system_health_session_path = @system_health_session_basepath + 'system_health*.xel'
			end
	end
else
	begin
		declare @system_health_session_fileExists as int = 0;

		exec master.dbo.xp_fileexist @system_health_session_file, @system_health_session_fileExists output;
		if ( @system_health_session_fileExists = 1 )
			begin
				set @system_health_session_path = @system_health_session_file
			end
		else
			begin
				raiserror ('System Health File does not exist',16,1)
			end
	end;

/* Get XE Data */
begin

	if @store_in_database is not null and @date_from = '1900-01-01'
		begin
			declare @t table (
				[Event Time] datetime
			);

			set @sql = 'select convert(datetime,dateadd(second,1,max([Event Time]))) from [' + @store_in_database + '].[dbo].[SystemHealthCheck_Diagnostics] where [Sql Instance] = @@SERVERNAME'

			insert into @t
			execute (@sql)

			select top 1 @date_from = [Event Time] from @t;

			if @date_from is null
				begin
					set @date_from = convert(datetime,'2000-01-01')
				end
		end

	/* AlwaysOn Health */
	begin
		if object_id('tempdb..##hadr_db_partner_set_sync_state') is not null
			drop table ##hadr_db_partner_set_sync_state

		select
			event_data = convert(xml, event_data)
		into ##hadr_db_partner_set_sync_state
		from sys.fn_xe_file_target_read_file(@always_on_session_path, null, null, null)
		where object_name = 'hadr_db_partner_set_sync_state'
		and convert(datetime,substring(event_data,PATINDEX('%timestamp%',event_data)+len('timestamp="'),24)) between @date_from and @date_to
	
		if object_id('tempdb..##alwayson_ddl_executed') is not null
			drop table ##alwayson_ddl_executed

		select 
			event_data = convert(xml, event_data)
		into ##alwayson_ddl_executed
		from sys.fn_xe_file_target_read_file(@always_on_session_path, null, null, null)
		where object_name = 'alwayson_ddl_executed'
		and convert(datetime,substring(event_data,PATINDEX('%timestamp%',event_data)+len('timestamp="'),24)) between @date_from and @date_to

		if object_id('tempdb..##alwayson_error_reported') is not null
			drop table ##alwayson_error_reported

		select 
			event_data = convert(xml, event_data)
		into ##alwayson_error_reported
		from sys.fn_xe_file_target_read_file(@always_on_session_path, null, null, null)
		where object_name = 'error_reported'
		and convert(datetime,substring(event_data,PATINDEX('%timestamp%',event_data)+len('timestamp="'),24)) between @date_from and @date_to
	end

	/* System Health */
	begin
		
		/* sp_server_diagnostics_component_result */
		begin
			if object_id('tempdb..##sp_server_diagnostics_component_result') is not null
				drop table ##sp_server_diagnostics_component_result

			select 
				event_data = convert(xml, event_data)
			into ##sp_server_diagnostics_component_result
			from sys.fn_xe_file_target_read_file(@system_health_session_path, null, null, null)
			where object_name = 'sp_server_diagnostics_component_result'
			and convert(datetime,substring(event_data,PATINDEX('%timestamp%',event_data)+len('timestamp="'),24)) between @date_from and @date_to
		end

		/* scheduler_monitor_system_health_ring_buffer_recorded */
		begin
			if object_id('tempdb..##scheduler_monitor_system_health_ring_buffer_recorded') is not null
				drop table ##scheduler_monitor_system_health_ring_buffer_recorded

			select 
				event_data = convert(xml, event_data)
			into ##scheduler_monitor_system_health_ring_buffer_recorded
			from sys.fn_xe_file_target_read_file(@system_health_session_path, null, null, null)
			where object_name = 'scheduler_monitor_system_health_ring_buffer_recorded'
			and convert(datetime,substring(event_data,PATINDEX('%timestamp%',event_data)+len('timestamp="'),24)) between dateadd(minute,-6,@date_from) and @date_to
		end

		/* wait_info */
		begin
			if object_id('tempdb..##wait_info') is not null
				drop table ##wait_info

			select
				event_data = convert(xml, event_data)
			into ##wait_info
			from sys.fn_xe_file_target_read_file(@system_health_session_path, null, null, null)
			where object_name = 'wait_info'
			and convert(datetime,substring(event_data,PATINDEX('%timestamp%',event_data)+len('timestamp="'),24)) between @date_from and @date_to
		end

		/* wait_info_external */
		begin
			if object_id('tempdb..##wait_info_external') is not null
				drop  table ##wait_info_external

			select
				event_data = convert(xml, event_data)
			into ##wait_info_external
			from sys.fn_xe_file_target_read_file(@system_health_session_path, null, null, null)
			where object_name = 'wait_info_external'
			and convert(datetime,substring(event_data,PATINDEX('%timestamp%',event_data)+len('timestamp="'),24)) between @date_from and @date_to
		end

		/* memory_broker_ring_buffer_recorded */
		begin
			if object_id('tempdb..##memory_broker_ring_buffer_recorded') is not null
				drop  table ##memory_broker_ring_buffer_recorded

			select
				event_data = convert(xml, event_data)
			into ##memory_broker_ring_buffer_recorded
			from sys.fn_xe_file_target_read_file(@system_health_session_path, null, null, null)
			where object_name = 'memory_broker_ring_buffer_recorded'
			and convert(datetime,substring(event_data,PATINDEX('%timestamp%',event_data)+len('timestamp="'),24)) between @date_from and @date_to
		end

		/* error_reported */
		begin
			if object_id('tempdb..##error_reported') is not null
				drop  table ##error_reported

			select
				event_data = convert(xml, event_data)
			into ##error_reported
			from sys.fn_xe_file_target_read_file(@system_health_session_path, null, null, null)
			where object_name = 'error_reported'
			and convert(datetime,substring(event_data,PATINDEX('%timestamp%',event_data)+len('timestamp="'),24)) between @date_from and @date_to
		end

		/* connectivity_ring_buffer_recorded */
		/*
		begin
			if object_id('tempdb..##connectivity_ring_buffer_recorded') is not null
				drop  table ##connectivity_ring_buffer_recorded

			select
				event_data = convert(xml, event_data)
			into ##connectivity_ring_buffer_recorded
			from sys.fn_xe_file_target_read_file(@system_health_session_path, null, null, null)
			where object_name = 'connectivity_ring_buffer_recorded'
			and convert(datetime,substring(event_data,PATINDEX('%timestamp%',event_data)+len('timestamp="'),24)) between @date_from and @date_to
		end
		*/
	end

end

/* Process XE Data */
begin

	/* Ring Buffer CPU */
	begin
		if object_id('tempdb..##ringbuffercpu') is not null
			drop table ##ringbuffercpu;

		select
			"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
			"Event Data" = x.event_data.query('.')
		into ##ringbuffercpu
		from  ##scheduler_monitor_system_health_ring_buffer_recorded t
		cross apply t.event_data.nodes('event') as x (event_data)
	end

	/* Query Processing */
	begin
		if object_id('tempdb..##queryprocessing') is not null
			drop table ##queryprocessing;

		select
			"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
			"Max Workers" = dxml.value('(./@maxWorkers)[1]','bigint'),
			"Workers Created" = dxml.value('(./@workersCreated)[1]','bigint'),
			"Workers Idle" = dxml.value('(./@workersIdle)[1]','bigint'),
			"Pending Tasks" = dxml.value('(./@pendingTasks)[1]','bigint'),
			RN = ROW_NUMBER() OVER (order by convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime'))),
			[Prev Event Time] = convert(smalldatetime,null)
		into ##queryprocessing
		from ##sp_server_diagnostics_component_result t
		cross apply t.event_data.nodes('event') as x (event_data)
		cross apply x.event_data.nodes('./data[@name="data"]/value/queryProcessing') as d(dxml)

		update q1
			set q1.[Prev Event Time] = isnull(q2.[Event Time],@date_from)
		from ##queryprocessing q1
		left join ##queryprocessing q2
			on q2.RN = q1.RN - 1

	end

	/* System */
	begin
		if object_id('tempdb..##system') is not null
			drop table ##system

		select
			"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
			"Os Cpu Utilization" = dxml.value('(./@systemCpuUtilization)[1]','bigint') - dxml.value('(./@sqlCpuUtilization)[1]','bigint'),
			"Sql Cpu Utilization" = dxml.value('(./@sqlCpuUtilization)[1]','bigint'),
			"Page Faults" = dxml.value('(./@pageFaults)[1]','int'),
			"Bad Pages Detected" = dxml.value('(./@BadPagesDetected)[1]','int'),
			"Bad Pages Fixed" = dxml.value('(./@BadPagesFixed)[1]','int')
		into ##system
		from ##sp_server_diagnostics_component_result t
		cross apply t.event_data.nodes('event') as x (event_data)
		cross apply x.event_data.nodes('./data[@name="data"]/value/system') as d(dxml)
	end

	/* Resource */
	begin
		if object_id('tempdb..##resource') is not null
			drop table ##resource;

		select
			"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
			"Out Of Memory Exceptions" = dxml.value('(./@outOfMemoryExceptions)[1]','bigint'),
			"Process Out Of Memory Period" = dxml.value('(./@processOutOfMemoryPeriod)[1]','bigint')
		into ##resource
		from ##sp_server_diagnostics_component_result t
		cross apply t.event_data.nodes('event') as x (event_data)
		cross apply x.event_data.nodes('./data[@name="data"]/value/resource') as d(dxml)
	end

	/* Memory System Process */
	begin
		if object_id('tempdb..##memorySystemProcess') is not null
			drop table ##memorySystemProcess;

		select 
			"Event Time",
			"Description" = w.r.value('(./@description)[1]','varchar(255)'),
			"Value" = w.r.value('(./@value)[1]','bigint')
		into ##memorySystemProcess
		from (
			select
				"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
				memoryReport = dxml.query('.')
			from ##sp_server_diagnostics_component_result t
			cross apply t.event_data.nodes('event') as x (event_data)
			cross apply x.event_data.nodes('./data[@name="data"]/value/resource/memoryReport[@name="Process/System Counts"]/entry') as d(dxml)
		) t
		cross apply t.memoryReport.nodes('entry') as w(r)
	end

	/* Memory Manager */
	begin
		if object_id('tempdb..##memoryManager') is not null
			drop table ##memoryManager;

		select 
			"Event Time",
			"Description" = w.r.value('(./@description)[1]','varchar(255)'),
			"Value" = w.r.value('(./@value)[1]','bigint')
		into ##memoryManager
		from (
			select
				"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
				memoryReport = dxml.query('.')
			from ##sp_server_diagnostics_component_result t
			cross apply t.event_data.nodes('event') as x (event_data)
			cross apply x.event_data.nodes('./data[@name="data"]/value/resource/memoryReport[@name="Memory Manager"]/entry') as d(dxml)
		) t
		cross apply t.memoryReport.nodes('entry') as w(r)
	end

	/* IO Subsystem */
	begin
		if object_id('tempdb..##iosubsystem') is not null
			drop table ##iosubsystem;

		select
			"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
			"IO Latch Timeouts" = dxml.value('(./@ioLatchTimeouts)[1]','bigint'),
			"Interval Long IOs" = dxml.value('(./@intervalLongIos)[1]','bigint'),
			"Total Long IOs" = dxml.value('(./@totalLongIos)[1]','bigint')
		into ##iosubsystem
		from ##sp_server_diagnostics_component_result t
		cross apply t.event_data.nodes('event') as x (event_data)
		cross apply x.event_data.nodes('./data[@name="data"]/value/ioSubsystem') as d(dxml)
	end

	/* Longest Pending Requests */
	begin
		if object_id('tempdb..##longestPendingRequests') is not null
			drop table ##longestPendingRequests;

		select
			"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
			pendingRequests = dxml.query('.')
		into ##longestPendingRequests
		from ##sp_server_diagnostics_component_result t
		cross apply t.event_data.nodes('event') as x (event_data)
		cross apply x.event_data.nodes('./data[@name="data"]/value/ioSubsystem/longestPendingRequests') as d(dxml)


		if object_id('tempdb..##longestPendingRequestsDetails') is not null
			drop table ##longestPendingRequestsDetails;

		select
			"Event Time",
			"Duration" = w.r.value('(./@duration)[1]','bigint'),
			"File Path" = w.r.value('(./@system_health_session_filePath)[1]','varchar(255)'),
			"Offset" = w.r.value('(./@offset)[1]','bigint'),
			"Handle" = w.r.value('(./@handle)[1]','varchar(255)')
		into ##longestPendingRequestsDetails
		from ##longestPendingRequests t
		cross apply t.pendingRequests.nodes('longestPendingRequests/pendingRequest') as w(r)
	end

	/* Top Waits */
	begin
		if object_id('tempdb..##querywaits') is not null
			drop table ##querywaits

		select
			"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
			topWaits = dxml.query('.')
		into ##querywaits
		from ##sp_server_diagnostics_component_result t
		cross apply t.event_data.nodes('event') as x (event_data)
		cross apply x.event_data.nodes('./data[@name="data"]/value/queryProcessing/topWaits') as d(dxml)

		if object_id('tempdb..##waitdetails') is not null
			drop table ##waitdetails;

		select [Event Time]
			, [Wait Type] = w.r.value('(./@waitType)[1]','varchar(255)')
			, [Waits] = w.r.value('(./@waits)[1]','bigint')
			, [Average Wait Time] = w.r.value('(./@averageWaitTime)[1]','bigint')
			, [Max Wait Time] = w.r.value('(./@maxWaitTime)[1]','bigint')
			, TopWaitsGroup = 'nonPreemptive byDuration'
			, RN = ROW_NUMBER() over (partition by w.r.value('(./@waitType)[1]','varchar(255)') ORDER BY "Event Time")
		into ##waitdetails
		from ##querywaits q
		cross apply q.topWaits.nodes('topWaits/nonPreemptive/byDuration/wait') as w(r)

		if object_id('tempdb..##waitdeltas') is not null
			drop table ##waitdeltas;

		select w2."Event Time"
			, w2."Wait Type"
			, "Average Wait Time Delta" = case when w2."Average Wait Time" > w1."Average Wait Time" then w2."Average Wait Time" - w1."Average Wait Time" else 0 end
			, "Average Wait Time" = w1."Average Wait Time"
			, "Max Wait Time Delta" = case when w2."Max Wait Time" > w1."Max Wait Time" then w2."Max Wait Time" - w1."Max Wait Time" else 0 end
			, "Max Wait Time" = w1."Max Wait Time"
		into ##waitdeltas
		from ##waitdetails w1
		left join ##waitdetails w2
			on w1."Wait Type" = w2."Wait Type"
			and w1.TopWaitsGroup = w2.TopWaitsGroup
			and w1.RN = w2.RN - 1
		where w2."Event Time" is not null
	end

	/* CPU Intensive requests */
	begin
		if object_id('tempdb..##cpuIntensiveRequests') is not null
			drop table ##cpuIntensiveRequests
			begin
				select
					"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
					cpuIntensiveRequests = dxml.query('.')
				into ##cpuIntensiveRequests
				from ##sp_server_diagnostics_component_result t
				cross apply t.event_data.nodes('event') as x (event_data)
				cross apply x.event_data.nodes('./data[@name="data"]/value/queryProcessing/cpuIntensiveRequests') as d(dxml)
			end
		;

		if object_id('tempdb..##cpuintensiverequestsDetails') is not null
			drop table ##cpuintensiverequestsDetails

		select t.[Event Time]
				,sessionId= w.r.value('(./@sessionId)[1]','int')
				,requestId= w.r.value('(./@requestId)[1]','int') 
				,command= w.r.value('(./@command)[1]','varchar(255)')
				,taskAddress= w.r.value('(./@taskAddress)[1]','varchar(255)')
				,cpuUtilization= w.r.value('(./@cpuUtilization)[1]','bigint')
				,cpuTimeMs= w.r.value('(./@cpuTimeMs)[1]','bigint')
		into ##cpuintensiverequestsDetails
		from ##cpuIntensiveRequests t
		cross apply t.cpuIntensiveRequests.nodes('cpuIntensiveRequests/request') as w(r)
	end

	/* Pending tasks */
	begin
		if object_id('tempdb..##pendingtasks') is not null
			drop table ##pendingtasks;

		select
			"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
			pendingTasks = dxml.query('.')
		into ##pendingtasks
		from ##sp_server_diagnostics_component_result t
		cross apply t.event_data.nodes('event') as x (event_data)
		cross apply x.event_data.nodes('./data[@name="data"]/value/queryProcessing/pendingTasks') as d(dxml)
	end

	/* Blocking tasks */
	begin
		if object_id('tempdb..##blockingtasks') is not null
			drop table ##blockingtasks;

		select
			"Event Time" =  convert(smalldatetime,x.event_data.value('(@timestamp)[1]', 'datetime')),
			blockingTasks = dxml.query('.')
		into ##blockingtasks
		from ##sp_server_diagnostics_component_result t
		cross apply t.event_data.nodes('event') as x (event_data)
		cross apply x.event_data.nodes('./data[@name="data"]/value/queryProcessing/blockingTasks') as d(dxml)
	end

	/* Results */
	begin
		set @sql = case when @store_in_database is not null then '
		insert into [' + @store_in_database + '].[dbo].[SystemHealthCheck_Diagnostics]' else '' end + '
		select [Sql Instance] = @@SERVERNAME, qp.[Event Time], qp.[Max Workers], qp.[Pending Tasks], qp.[Workers Created], qp.[Workers Idle]
			, s.[Sql Cpu Utilization], s.[Os Cpu Utilization]
				
			' + case when @include_xml_columns = 1 then ', [Scheduler Monitor Events] = convert(xml,(
					select event_data = "Event Data"
					from ##ringbuffercpu
					where [Event Time] >= qp.[Prev Event Time] and [Event Time] < qp.[Event Time]
					order by [Event Time]
					for xml path(''''), root(''scheduler_monitor_events''), type
				))
			' else '' end + '
	
			, s.[Bad Pages Detected], s.[Bad Pages Fixed]
			, r.[Out Of Memory Exceptions], r.[Process Out Of Memory Period]
			, ios.[IO Latch Timeouts], ios.[Interval Long IOs], ios.[Total Long IOs]

			, m.[Available Physical Memory]
			, m.[Available Virtual Memory]
			, m.[Available Paging File]
			, m.[Working Set]
			, m.[Percent of Committed Memory in WS]
			, m.[Page Faults]
			, m.[System physical memory high]
			, m.[System physical memory low]
			, m.[Process physical memory low]
			, m.[Process virtual memory low]
			, m.[VM Reserved]
			, m.[VM Committed]
			, m.[Locked Pages Allocated]
			, m.[Large Pages Allocated]
			, m.[Emergency Memory]
			, m.[Emergency Memory In Use]
			, m.[Target Committed]
			, m.[Current Committed]
			, m.[Pages Allocated]
			, m.[Pages Reserved]
			, m.[Pages Free]
			, m.[Pages In Use]
			, m.[Page Alloc Potential]
			, m.[NUMA Growth Phase]
			, m.[Last OOM Factor]
			, m.[Last OS Error]

			, [Long Pending Requests Count] = isnull([Long Pending Requests Count],0)
			, [Long Pending Requests Total Duration] = isnull(lpr.[Long Pending Requests Total Duration],0)
			' + case when @include_xml_columns = 1 then ', [Long Pending Requests] = convert(xml,(
				select pendingRequests
				from ##longestPendingRequests r where r.[Event Time] = qp.[Event Time])
				)' else '' end + '

			, [Average Wait Time] = isnull([Average Wait Time],0)
			, [Average Wait Time Delta] = isnull([Average Wait Time Delta],0)
			, "Max Wait Time" = isnull("Max Wait Time",0)
			, "Max Wait Time Delta" = isnull("Max Wait Time Delta",0)
			' + case when @include_xml_columns = 1 then ', [Top Waits List] = convert(xml,(
				select topWaits
				from ##querywaits r where r.[Event Time] = qp.[Event Time])
				)' else'' end + '

			, [Intensive Queries Count] = isnull([Intensive Queries Count],0)
			, [Intensive Queries Total Cpu Utilisation] = isnull([Intensive Queries Total Cpu Utilisation],0)
			, [Intensive Queries Total Cpu ms] = isnull([Intensive Queries Total Cpu Ms],0)

			' + case when @include_xml_columns = 1 then ', [CPU Intensive Requests List] = convert(xml,(
				select cpuintensiverequests
				from ##cpuintensiverequests r where r.[Event Time] = qp.[Event Time] )
				)' else'' end + '

			' + case when @include_xml_columns = 1 then ', [Pending Tasks List] = convert(xml,(
				select pendingTasks
				from ##pendingtasks r where r.[Event Time] = qp.[Event Time])
				)' else'' end + '

			, [Blocking Tasks List] = isnull([Blocking Tasks],0)
			' + case when @include_xml_columns = 1 then ', [Blocked Process Report] = convert(xml,(
				select blockingTasks 
				from ##blockingtasks r where r.[Event Time] = qp.[Event Time])
				)' else'' end + '

			, [Event hour] =  DATEPART(hour,qp.[Event Time])
		from ##queryprocessing qp

		inner join ##system s
			on s.[Event Time] = qp.[Event Time]

		inner join ##resource r
			on r.[Event Time] = qp.[Event Time]

		inner join ##iosubsystem ios
			on ios.[Event Time] = qp.[Event Time]

		left join (
			select [Event Time]
				, [Blocking Tasks] = count(r.query(''.''))
			from ##blockingtasks t
			cross apply t.blockingTasks.nodes(''blockingTasks/blocked-process-report'') as w(r)
			group by [Event Time]
		) blockingtasks
		on blockingtasks.[Event Time] = qp.[Event Time]

		left join (
			select [Event Time]
				,[Average Wait Time Delta] = avg([Average Wait Time Delta])
				,[Average Wait Time] = avg([Average Wait Time])
				,"Max Wait Time" = max("Max Wait Time")
				,"Max Wait Time Delta" = max("Max Wait Time Delta")
			from ##waitdeltas
			group by [Event Time]
		) nonpreemptivewaitstotal
		on nonpreemptivewaitstotal.[Event Time] = qp.[Event Time]

		left join (
			select [Event Time]
				, [Intensive Queries Total Cpu Utilisation]=sum(cpuUtilization)
				, [Intensive Queries Total Cpu Ms]=sum(cpuTimeMs)
				, [Intensive Queries Count]=count(*)
			from ##cpuintensiverequestsDetails
			group by [Event Time]
		) cpu
		on cpu.[Event Time] = qp.[Event Time]
		
		left join (
			select [Event Time]
				, [Long Pending Requests Total Duration] = sum(Duration)
				, [Long Pending Requests Count] = count(*)
			from ##longestPendingRequestsDetails
			group by [Event Time]
			) lpr
			on lpr.[Event Time] = qp.[Event Time]

		left join (
			select [Event Time],  
						[Available Physical Memory]
					,[Available Virtual Memory]
					,[Available Paging File]
					,[Working Set]
					,[Percent of Committed Memory in WS]
					,[Page Faults]
					,[System physical memory high]
					,[System physical memory low]
					,[Process physical memory low]
					,[Process virtual memory low]
					,[VM Reserved]
					,[VM Committed]
					,[Locked Pages Allocated]
					,[Large Pages Allocated]
					,[Emergency Memory]
					,[Emergency Memory In Use]
					,[Target Committed]
					,[Current Committed]
					,[Pages Allocated]
					,[Pages Reserved]
					,[Pages Free]
					,[Pages In Use]
					,[Page Alloc Potential]
					,[NUMA Growth Phase]
					,[Last OOM Factor]
					,[Last OS Error]
			from  
				(
				select [Event Time], Description, Value
				from ##memorySystemProcess
				union all
				select [Event Time], Description, Value
				from ##memoryManager			
				) as mem
			pivot  
			(  
				max(Value) for Description   
				IN ( 
						[Available Physical Memory]
					,[Available Virtual Memory]
					,[Available Paging File]
					,[Working Set]
					,[Percent of Committed Memory in WS]
					,[Page Faults]
					,[System physical memory high]
					,[System physical memory low]
					,[Process physical memory low]
					,[Process virtual memory low]
					,[VM Reserved]
					,[VM Committed]
					,[Locked Pages Allocated]
					,[Large Pages Allocated]
					,[Emergency Memory]
					,[Emergency Memory In Use]
					,[Target Committed]
					,[Current Committed]
					,[Pages Allocated]
					,[Pages Reserved]
					,[Pages Free]
					,[Pages In Use]
					,[Page Alloc Potential]
					,[NUMA Growth Phase]
					,[Last OOM Factor]
					,[Last OS Error]
				)
			) p
		) m
			on m.[Event Time] = qp.[Event Time]
		order by [Event Time]'

		exec ( @sql );

		set @sql = case when @store_in_database is not null and isnull(@retention_days,0) > 0 then '
			delete from [' + @store_in_database + '].[dbo].[SystemHealthCheck_Diagnostics]
			where [Event Time] < dateadd(day,-' + convert(varchar(10),@retention_days) + ',getutcdate())
		' end

		if @sql is not null
			exec (@sql)
	end
end
