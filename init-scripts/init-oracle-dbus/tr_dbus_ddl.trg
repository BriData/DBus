create or replace trigger sys.tr_dbus_ddl
  after ddl or comment on database
declare
  v_event_id   number;
  v_version    number;
  v_ddl_type   varchar2(64);
  v_dist_owner varchar2(64) := ora_dict_obj_owner;
  v_dist_table varchar2(64) := ora_dict_obj_name;

  /**get ddl*/
  function get_ddl return varchar2 is
    n        pls_integer;
    sql_text ora_name_list_t;
    ddl      varchar2(3000);
  begin
    n := ora_sql_txt(sql_text);
    for i in 1 .. n loop
      if length(trim(sql_text(i))) > 0 then
        if instr(sql_text(i), '--') = 1 then
          ddl := ddl || substr(sql_text(i), instr(sql_text(i), chr(10)), length(sql_text(i)));
        else
          ddl := ddl || sql_text(i);
        end if;
      end if;
    end loop;
    ddl := trim(substr(upper(regexp_replace(ddl, '\s+', chr(32))), 1, 3000));
    return ddl;
  exception
    when others then
      return null;
  end get_ddl;

  function base64_encode(p_txt varchar2) return varchar2 is
  begin
    return regexp_replace(utl_raw.cast_to_varchar2(utl_encode.BASE64_ENCODE(utl_raw.cast_to_raw(p_txt))), '\s+', '');
  end base64_encode;

  function build_json(p_target varchar2, p_ddl varchar2) return varchar2 is
  begin
    return '{target:"' || p_target || '", version:"1", ddl:"' || p_ddl || '"}';
  end build_json;

  function json_width_ddl(p_target varchar2) return varchar2 is
  begin
    return build_json(p_target, base64_encode(get_ddl()));
  end json_width_ddl;

  -- create meta sync event
  procedure create_event(p_owenr varchar2, p_dist_name varchar2, p_event_id number, p_ddltype varchar2, p_version number, p_ddl varchar2) is
    pragma autonomous_transaction;
  begin
    insert into dbus.meta_sync_event
      (serno, table_owner, table_name, version, event_time, ddl_type, ddl)
    values
      (p_event_id, p_owenr, p_dist_name, p_version, systimestamp, p_ddltype, p_ddl);
    commit;
  end create_event;

  /** 判断是否为dbus.dbus_tables中配置的表 */
  function is_dbus_table(p_owner varchar2, p_table_name varchar2) return boolean is
    v_count int;
  begin
    select count(1)
      into v_count
      from dual
     where exists (select 1
              from dbus.dbus_tables t
             where t.owner = p_owner
               and t.table_name = p_table_name);
    return v_count != 0;
  end is_dbus_table;
begin

  --truncate table
  if (ora_sysevent = 'TRUNCATE' and ora_dict_obj_type = 'TABLE') then
    if is_dbus_table(v_dist_owner, v_dist_table) then
      --create truncate event for meta data sync
      select dbus.seq_ddl_version.nextval into v_event_id from dual;
      create_event(v_dist_owner, v_dist_table, v_event_id, 'TRUNCATE', -1, json_width_ddl(v_dist_table));
    end if;
    return;
  end if;

  -- 判断是否为添加注释
  if ora_sysevent = 'COMMENT' and (ora_dict_obj_type = 'TABLE' or ora_dict_obj_type = 'COLUMN') then
    -- 如果是对列加索引则截取出table
    if ora_dict_obj_type = 'COLUMN' then
      v_dist_table := substr(ora_dict_obj_name, 0, instr(ora_dict_obj_name, '.') - 1);
    end if;
  
    if is_dbus_table(v_dist_owner, v_dist_table) then
      v_ddl_type := 'COMMENT-' || ora_dict_obj_type;
    
      select dbus.seq_ddl_version.nextval into v_event_id from dual;
      create_event(v_dist_owner, v_dist_table, v_event_id, v_ddl_type, -1, json_width_ddl(ora_dict_obj_name));
    end if;
    return;
  end if;

  if (ora_sysevent = 'ALTER' and ora_dict_obj_type = 'TABLE') then
    if is_dbus_table(v_dist_owner, v_dist_table) = false then
      return;
    end if;
  
    v_ddl_type := 'ALTER';
    --get next version number
    select decode(max(t.version), null, 0, max(t.version)) + 1
      into v_version
      from dbus.table_meta_his t
     where t.owner = v_dist_owner
       and t.table_name = v_dist_table;
  
    insert into dbus.table_meta_his
      (owner,
       table_name,
       column_name,
       data_type,
       data_length,
       data_precision,
       data_scale,
       nullable,
       version,
       ddl_time,
       column_id,
       internal_column_id,
       hidden_column,
       virtual_column,
       is_pk,
       pk_position,
       char_length,
       char_used,
       comments)
      select t1.owner,
             t1.table_name,
             t1.column_name,
             t1.data_type,
             t1.data_length,
             t1.data_precision,
             t1.data_scale,
             t1.nullable,
             v_version,
             systimestamp,
             t1.column_id,
             t1.internal_column_id,
             t1.hidden_column,
             t1.virtual_column,
             decode(t2.is_pk, 1, 'Y', 'N'),
             decode(t2.position, null, -1, t2.position),
             t1.char_length,
             t1.char_used,
             tcc.comments
        from (select *
                from all_tab_cols t
               where t.owner = v_dist_owner
                 and t.table_name = v_dist_table) t1
        left join all_col_comments tcc on (t1.owner = tcc.owner and t1.table_name = tcc.table_name and t1.column_name = tcc.column_name)
        left join (select cu.owner, cu.table_name, cu.column_name, cu.position, 1 as is_pk
                     from all_cons_columns cu, all_constraints au
                    where cu.constraint_name = au.constraint_name
                      and cu.owner = au.owner
                      and au.constraint_type = 'P'
                      and au.table_name = v_dist_table
                      and au.owner = v_dist_owner) t2 on (t1.column_name = t2.column_name and t1.table_name = t2.table_name and t1.owner = t2.owner);
    -- create event
    select dbus.seq_ddl_version.nextval into v_event_id from dual;
    create_event(v_dist_owner, v_dist_table, v_event_id, v_ddl_type, v_version, json_width_ddl(v_dist_table));
  end if;
exception
  when others then
    raise_application_error(num => -20000, msg => 'Trigger [tr_dbus_ddl] error. Sql_code:' || sqlcode || ' sqlerrm:' || sqlerrm);
end tr_dbus_ddl;
/

