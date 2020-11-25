import time
import logging
import os
import uuid
from pathlib import Path
from farm.data_handler.data_silo import DataSilo
from farm.data_handler.processor import NERProcessor
from farm.modeling.optimization import initialize_optimizer
from farm.infer import Inferencer
from farm.modeling.adaptive_model import AdaptiveModel
from farm.modeling.language_model import LanguageModel
from farm.modeling.prediction_head import TokenClassificationHead
from farm.modeling.tokenization import Tokenizer
from farm.train import Trainer
from farm.utils import set_all_seeds, MLFlowLogger, initialize_device_settings
from aihandler.ai_tsk import TSK


class NER(TSK):

    def __init__(self, db, s3, orcomm):
        TSK.__init__(self, db, s3, orcomm)
        self._taskKind = 'ner'
        self.useRemote = True
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(name)s -   %(message)s",
            datefmt="%m/%d/%Y %H:%M:%S",
            level=logging.INFO,
        )
        self.startTimer()
        

    def execML(self, job):
        print(job)
        if job.task == 'analyse':
            start_time = time.time()
            basic_texts = []
            print('INFO:', 'will donwload and store dataset...', flush=True)
            sample = self.downloadAndConvertText(job, job.data_sample)
            for text in sample.encode('utf-8').splitlines():
                basic_texts.append({ 'text': text.decode('utf-8') })
            #print(basic_texts)
            
            print('INFO:', 'will donwload and store model...', flush=True)
            
            
            self.downloadAndStoreZIPModel(job, job.model)
            self.updateJobStatus(job, 'analysing')
            save_dir = 'tmp/' + job.model['id']
            model = Inferencer.load(save_dir)
            result = model.inference_from_dicts(dicts=basic_texts)
            self.persistResult(job, result)
            print(str(result).encode('utf-8'))
            model.close_multiprocessing_pool()
            self.updateJobStatus(job, 'completed')
            elapsed_time = time.time() - start_time
            print('Execution time max: ', elapsed_time,
                  'for job.id:', job.id,  flush=True)
        elif job.task == 'train':
            self.updateJobStatus(job, 'training')
            start_time = time.time()
            print('INFO:', 'will donwload and store dataset...', flush=True)
            self.downloadAndStoreZIPDataset(job, job.data_source)
            print('INFO:', 'will donwload and store model...', flush=True)
            self.downloadAndStoreZIPModel(job, job.model)
            set_all_seeds(seed=42)
            device, n_gpu = initialize_device_settings(use_cuda=True)
            n_epochs = 4
            evaluate_every = 400
            do_lower_case = False
            batch_size = 32
            lang_model = os.path.join(Path.cwd(), 'tmp', job.model['id'])
            ner_labels = ["[PAD]", "X", "O", "B-MISC", "I-MISC", "B-PER",
                        "I-PER", "B-ORG", "I-ORG", "B-LOC", "I-LOC", "B-OTH", "I-OTH"]
            # 1.>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Create a tokenizer
            tokenizer = Tokenizer.load(pretrained_model_name_or_path=lang_model, do_lower_case=do_lower_case, tokenizer_class='BertTokenizer') #tokenizer_class='BertTokenizer'
            # 2. >>>>>>>>>>>>>>>>>>>>>>>>>>>>> Create a DataProcessor that handles all the conversion from raw text into a pytorch Dataset
            # See test/sample/ner/train-sample.txt for an example of the data format that is expected by the Processor
            processor = NERProcessor(tokenizer=tokenizer, max_seq_len=128, data_dir=str(os.path.join(Path.cwd(), 'tmp', job.data_source['id'])), delimiter=' ', metric='seq_f1', label_list=ner_labels)
            # 3. >>>>>>>>>>>>>>>>>>>>>>>>>>>>> Create a DataSilo that loads several datasets (train/dev/test), provides DataLoaders for them and calculates a few descriptive statistics of our datasets
            data_silo = DataSilo(processor=processor, batch_size=batch_size, max_processes=1)
            # 4. >>>>>>>>>>>>>>>>>>>>>>>>>>>>> Create an AdaptiveModel 
            # a) which consists of a pretrained language model as a basis
            language_model = LanguageModel.load(lang_model)
            # b) and a prediction head on top that is suited for our task => NER
            prediction_head = TokenClassificationHead(num_labels=len(ner_labels))
            model = AdaptiveModel(
                language_model=language_model,
                prediction_heads=[prediction_head],
                embeds_dropout_prob=0.1,
                lm_output_types=['per_token'],
                device=device,
            )
            # 5. >>>>>>>>>>>>>>>>>>>>>>>>>>>>> Create an optimizer
            model, optimizer, lr_schedule = initialize_optimizer(
                model=model,
                learning_rate=1e-5,
                n_batches=len(data_silo.loaders["train"]),
                n_epochs=n_epochs,
                device=device,
            )
            # 6. >>>>>>>>>>>>>>>>>>>>>>>>>>>>> Feed everything to the Trainer, which keeps care of growing our model into powerful plant and evaluates it from time to time
            trainer = Trainer(
                model=model,
                optimizer=optimizer,
                data_silo=data_silo,
                epochs=n_epochs,
                n_gpu=n_gpu,
                lr_schedule=lr_schedule,
                evaluate_every=evaluate_every,
                device=device,
            )
            # 7. >>>>>>>>>>>>>>>>>>>>>>>>>>>>> Let it grow
            trainer.train()
            # 8. >>>>>>>>>>>>>>>>>>>>>>>>>>>>> Hooray! You have a model. Store it:
            newModelId = str(uuid.uuid4())
            save_dir = 'tmp/' + newModelId
            model.save(save_dir)
            processor.save(save_dir)
            model.close_multiprocessing_pool()
            self.persistZIPModel(newModelId, job)
            self.updateJobStatus(job, 'completed')
            elapsed_time = time.time() - start_time
            print('Execution time max: ', elapsed_time,
                  'for job.id:', job.id,  flush=True)
        # return True
        return {'status': True, 'code': 'ok', 'msg': 'success'}
