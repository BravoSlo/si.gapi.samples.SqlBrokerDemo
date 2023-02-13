use [master]
go

-- drop existing databases
if db_id('BrokerSource01') is not null
begin
	alter database [BrokerSource01] set single_user with rollback immediate
	use [master]
	drop database [BrokerSource01]
end
go
if db_id('BrokerSource02') is not null
begin
	alter database [BrokerSource02] set single_user with rollback immediate
	use [master]
	drop database [BrokerSource02]
end
go
if db_id('BrokerMain') is not null
begin
	alter database [BrokerMain] set single_user with rollback immediate
	use [master]
	drop database [BrokerMain]
end
go

-- create BrokerSource01
create database [BrokerSource01];
go
use [BrokerSource01]
go
alter database [BrokerSource01] set enable_broker;
go
alter database [BrokerSource01] set trustworthy on
go
create table [dbo].[TableSourceOne](
	[id] [int] identity(1,1) NOT NULL,
	[createdDate] [datetime] NOT NULL,
	[data] [varchar](250) NOT NULL,
	constraint [PK_TableSourceOne] primary key clustered ([id] asc)
)
alter table [dbo].[TableSourceOne] add constraint [DF_TableSourceOne_createdDate] default (getutcdate()) for [createdDate];
go
use [master]
go

-- create BrokerSource02
create database [BrokerSource02];
go
use [BrokerSource02]
go
alter database [BrokerSource02] set enable_broker;
go
alter database [BrokerSource02] set trustworthy on
go
create table [dbo].[TableSourceTwo](
	[id] [int] identity(1,1) NOT NULL,
	[createdDate] [datetime] NOT NULL,
	[data] [varchar](250) NOT NULL,
	constraint [PK_TableSourceTwo] primary key clustered ([id] asc)
)
alter table [dbo].[TableSourceTwo] add constraint [DF_TableSourceTwo_createdDate] default (getutcdate()) for [createdDate];
go
use [master]
go

-- create BrokerMain
create database [BrokerMain];
go
use [BrokerMain]
go
alter database [BrokerMain] set enable_broker;
go
alter database [BrokerMain] set trustworthy on
go
use [master]
go

-- create broker objects for BrokerSource01
use [BrokerSource01]
go
create message type TextMessageType validation = none
create contract TextContract (TextMessageType sent by initiator)
create queue BrokerSourceQueue with status = on
create service BrokerSourceService on queue BrokerSourceQueue (TextContract)
go

-- create broker objects for BrokerSource02
use [BrokerSource02]
go
create message type TextMessageType validation = none
create contract TextContract (TextMessageType sent by initiator)
create queue BrokerSourceQueue with status = on
create service BrokerSourceService on queue BrokerSourceQueue (TextContract)
go

-- create broker objects for BrokerMain
use [BrokerMain]
go
create message type TextMessageType validation = none
create contract TextContract (TextMessageType sent by initiator)
create queue BrokerTargetQueue with status = on
create service BrokerTargetService on queue BrokerTargetQueue (TextContract)
go

-- setup BrokerSource01
use [BrokerSource01]
go
create procedure [dbo].[BrokerSendMessage]
	@data varchar(1000)
as
begin
	set nocount on;
	declare @ch uniqueidentifier;
	begin transaction
		begin dialog @ch
		from service [BrokerSourceService]
		to service 'BrokerTargetService'
		on contract [TextContract]
		with encryption = off;

		send on conversation @ch
		message type [TextMessageType] (@data);

	commit transaction
end
go

-- setup BrokerSource01.TableSourceOne triggers
create trigger TableSourceOne_OnDelete
on [dbo].[TableSourceOne] 
after delete
as
begin
	declare @id int
	declare cur cursor local for
		select [id] from Deleted
	open cur
	fetch cur into @id
	while @@fetch_status > -1
	begin
		declare @message varchar(max);
		set @message = 'BrokerSource01;TableSourceOne;Delete;' + cast(@id as varchar(50));
		exec [dbo].[BrokerSendMessage] @data = @message
		fetch cur into @id
	end
	deallocate cur
end
go
create trigger TableSourceOne_OnInsert
on [dbo].[TableSourceOne] 
after insert
as
begin
	declare @id int
	declare cur cursor local for
		select [id] from Inserted
	open cur
	fetch cur into @id
	while @@fetch_status > -1
	begin
		declare @message varchar(max);
		set @message = 'BrokerSource01;TableSourceOne;Insert;' + cast(@id as varchar(50));
		exec [dbo].[BrokerSendMessage] @data = @message
		fetch cur into @id
	end
	deallocate cur
end
go
create trigger TableSourceOne_OnUpdate
on [dbo].[TableSourceOne] 
after update
as
begin
	declare @id int
	declare cur cursor local for
		select [id] from Inserted
	open cur
	fetch cur into @id
	while @@fetch_status > -1
	begin
		declare @message varchar(max);
		set @message = 'BrokerSource01;TableSourceOne;Update;' + cast(@id as varchar(50));
		exec [dbo].[BrokerSendMessage] @data = @message
		fetch cur into @id
	end
	deallocate cur
end
go

-- setup BrokerSource02
use [BrokerSource02]
go
create procedure [dbo].[BrokerSendMessage]
	@data varchar(1000)
as
begin
	set nocount on;
	declare @ch uniqueidentifier;
	begin transaction
		begin dialog @ch
		from service [BrokerSourceService]
		to service 'BrokerTargetService'
		on contract [TextContract]
		with encryption = off;

		send on conversation @ch
		message type [TextMessageType] (@data);

	commit transaction
end
go

-- setup BrokerSource02.TableSourceTwo triggers
create trigger TableSourceTwo_OnDelete
on [dbo].[TableSourceTwo] 
after delete
as
begin
	declare @id int
	declare cur cursor local for
		select [id] from Deleted
	open cur
	fetch cur into @id
	while @@fetch_status > -1
	begin
		declare @message varchar(max);
		set @message = 'BrokerSource02;TableSourceTwo;Delete;' + cast(@id as varchar(50));
		exec [dbo].[BrokerSendMessage] @data = @message
		fetch cur into @id
	end
	deallocate cur
end
go
create trigger TableSourceTwo_OnInsert
on [dbo].[TableSourceTwo] 
after insert
as
begin
	declare @id int
	declare cur cursor local for
		select [id] from Inserted
	open cur
	fetch cur into @id
	while @@fetch_status > -1
	begin
		declare @message varchar(max);
		set @message = 'BrokerSource02;TableSourceTwo;Insert;' + cast(@id as varchar(50));
		exec [dbo].[BrokerSendMessage] @data = @message
		fetch cur into @id
	end
	deallocate cur
end
go
create trigger TableSourceTwo_OnUpdate
on [dbo].[TableSourceTwo] 
after update
as
begin
	declare @id int
	declare cur cursor local for
		select [id] from Inserted
	open cur
	fetch cur into @id
	while @@fetch_status > -1
	begin
		declare @message varchar(max);
		set @message = 'BrokerSource02;TableSourceTwo;Update;' + cast(@id as varchar(50));
		exec [dbo].[BrokerSendMessage] @data = @message
		fetch cur into @id
	end
	deallocate cur
end
go

-- setup BrokerMain
use [BrokerMain]
go
create procedure [dbo].[BrokerMainReceiveMessage]
	@messageType varchar(500) OUT,
	@messageData varchar(1000) OUT
as
begin
	set nocount on;
	declare @ch uniqueidentifier;
	declare @type varchar(500);
	declare @body varchar(1000);
	begin transaction;
		waitfor(
			receive top (1)
				@ch = [conversation_handle],
				@type = [message_type_name],
				@body = [message_body]
			from [dbo].[BrokerTargetQueue]),
		timeout 5000;

		if (@@rowcount = 0)
		begin
			rollback transaction;
		end
		else 
		begin
			set @messageType = @type
			set @messageData = @body
			end conversation @ch
			commit transaction;
		end
end
go
