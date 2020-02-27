DR_EXPLAIN.namespace( 'DR_EXPLAIN.data_index' );
DR_EXPLAIN.data_index = {

	// index
	DREX_NODE_KEYWORDS: [4,5,6,12,7,3,1,8,9,14,13,2,10],
	DREX_NODE_KEYWORDS_START: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,5,5,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,11,13,13,13,13,13,13,13,13,13,13], //length:= drex.nodes_count,
	DREX_NODE_KEYWORDS_END: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,5,5,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,11,13,13,13,13,13,13,13,13,13,13,13], //length:= drex.nodes_count,

	DREX_KEYWORD_NAMES: ["<НОВОЕ КЛЮЧЕВОЕ СЛОВО>","connection","dataset","filemanager","репозиторий","имя объекта","группа объекта","forGroup","соединение","конфигурация","набор данных","менеджер файлов","регистрация","умолчанию","файл"],
	DREX_KEYWORD_CHILD_START: [1,12,12,12,12,13,13,13,13,14,15,15,15,15,15],
	DREX_KEYWORD_CHILD_END: [12,12,12,12,13,13,13,13,14,15,15,15,15,15,15]
	
};