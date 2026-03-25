import os
import json
from typing import List

import pandas as pd
import numpy as np
import torch
import torch.nn as nn

from transformers import AutoTokenizer, AutoModel


# =========================
# 현재 파일 기준 경로
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_MODEL_DIR = os.path.abspath(
    os.path.join(BASE_DIR, "../models", "econ_news_hierarchical")
)
DEFAULT_OUTPUT_DIR = os.path.abspath(
    os.path.join(BASE_DIR, "../../files/tagged")
)


# =========================
# 유틸
# =========================
def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).replace("\r", " ").replace("\n", " ").strip()


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_read_csv(path):
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(path, encoding="utf-8-sig", engine="python")

    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    return df


# =========================
# 모델 정의
# =========================
class HierarchicalClassifier(nn.Module):
    def __init__(self, model_name, num_major_labels, num_sub_labels):
        super().__init__()
        self.encoder = AutoModel.from_pretrained(model_name)
        hidden_size = self.encoder.config.hidden_size
        hidden_dropout_prob = getattr(self.encoder.config, "hidden_dropout_prob", 0.1)

        self.dropout = nn.Dropout(hidden_dropout_prob)
        self.major_classifier = nn.Linear(hidden_size, num_major_labels)
        self.sub_classifier = nn.Linear(hidden_size, num_sub_labels)

    def forward(self, input_ids=None, attention_mask=None, token_type_ids=None):
        outputs = self.encoder(
            input_ids=input_ids,
            attention_mask=attention_mask,
            token_type_ids=token_type_ids if token_type_ids is not None else None,
        )

        if hasattr(outputs, "pooler_output") and outputs.pooler_output is not None:
            pooled = outputs.pooler_output
        else:
            pooled = outputs.last_hidden_state[:, 0]

        pooled = self.dropout(pooled)

        major_logits = self.major_classifier(pooled)
        sub_logits = self.sub_classifier(pooled)

        return {
            "major_logits": major_logits,
            "sub_logits": sub_logits,
        }


# =========================
# 추론기
# =========================
class HierarchicalPredictor:
    def __init__(self, model_dir, device=None):
        self.model_dir = model_dir
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")

        config = load_json(os.path.join(model_dir, "label_config.json"))

        self.model_name = config["model_name"]
        self.max_length = config["max_length"]

        self.major_labels = config["major_labels"]
        self.sub_labels = config["sub_labels"]

        self.major_label2id = config["major_label2id"]
        self.sub_label2id = config["sub_label2id"]

        self.major_id2label = {int(k): v for k, v in config["major_id2label"].items()}
        self.sub_id2label = {int(k): v for k, v in config["sub_id2label"].items()}

        self.major_to_allowed_sub_ids = {
            int(k): v for k, v in config["major_to_allowed_sub_ids"].items()
        }

        self.tokenizer = AutoTokenizer.from_pretrained(model_dir, use_fast=False)

        self.model = HierarchicalClassifier(
            model_name=self.model_name,
            num_major_labels=len(self.major_labels),
            num_sub_labels=len(self.sub_labels),
        )

        state_dict_path = os.path.join(model_dir, "pytorch_model.bin")
        self.model.load_state_dict(torch.load(state_dict_path, map_location=self.device))
        self.model.to(self.device)
        self.model.eval()

    def build_text(self, title="", content=""):
        title = normalize_text(title)
        content = normalize_text(content)

        if not title and not content:
            raise ValueError("title/content 중 하나는 있어야 합니다.")

        return f"[제목] {title}\n\n[본문] {content}"

    def mask_sub_logits_by_major(self, sub_logits, major_pred_ids):
        masked = np.full_like(sub_logits, fill_value=-1e9)

        for i, major_id in enumerate(major_pred_ids):
            allowed_sub_ids = self.major_to_allowed_sub_ids[int(major_id)]
            masked[i, allowed_sub_ids] = sub_logits[i, allowed_sub_ids]

        return masked

    def predict(self, texts, batch_size=16):
        results = []

        for start in range(0, len(texts), batch_size):
            batch_texts = texts[start:start + batch_size]

            encoded = self.tokenizer(
                batch_texts,
                truncation=True,
                max_length=self.max_length,
                padding=True,
                return_tensors="pt",
            )
            encoded = {k: v.to(self.device) for k, v in encoded.items()}

            with torch.no_grad():
                outputs = self.model(**encoded)

            major_logits = outputs["major_logits"].cpu().numpy()
            sub_logits = outputs["sub_logits"].cpu().numpy()

            major_probs = torch.softmax(torch.tensor(major_logits), dim=-1).numpy()
            major_pred_ids = np.argmax(major_logits, axis=-1)

            masked_sub_logits = self.mask_sub_logits_by_major(sub_logits, major_pred_ids)
            sub_probs = torch.softmax(torch.tensor(masked_sub_logits), dim=-1).numpy()
            sub_pred_ids = np.argmax(masked_sub_logits, axis=-1)

            for i in range(len(batch_texts)):
                major_id = int(major_pred_ids[i])
                sub_id = int(sub_pred_ids[i])

                results.append({
                    "pred_major": self.major_id2label[major_id],
                    "pred_major_prob": float(major_probs[i][major_id]),
                    "pred_sub": self.sub_id2label[sub_id],
                    "pred_sub_prob": float(sub_probs[i][sub_id]),
                    "pred_label": f"{self.major_id2label[major_id]}>{self.sub_id2label[sub_id]}",
                })

        return results


# =========================
# CSV 처리
# =========================
def validate_input_columns(df):
    required_cols = {"title", "content"}
    if not required_cols.issubset(df.columns):
        raise ValueError("입력 CSV에는 반드시 title, content 컬럼이 있어야 합니다.")


def ensure_output_columns(df):
    df = df.copy()

    text_cols = ["pred_major", "pred_sub", "pred_label"]
    prob_cols = ["pred_major_prob", "pred_sub_prob"]

    for col in text_cols:
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].fillna("").astype(str)

    for col in prob_cols:
        if col not in df.columns:
            df[col] = np.nan
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def merge_existing_predictions(input_df, output_df):
    input_df = input_df.copy()
    output_df = ensure_output_columns(output_df.copy())

    pred_cols = ["pred_major", "pred_sub", "pred_label", "pred_major_prob", "pred_sub_prob"]

    if "item_id" in input_df.columns and "item_id" in output_df.columns:
        merge_cols = ["item_id"]
    else:
        merge_cols = ["title", "content"]

    output_keep = output_df[merge_cols + pred_cols].copy()
    merged = input_df.merge(output_keep, on=merge_cols, how="left")
    merged = ensure_output_columns(merged)
    return merged


def find_rows_to_predict(df):
    need_mask = (
        df["pred_major"].fillna("").astype(str).str.strip().eq("") |
        df["pred_sub"].fillna("").astype(str).str.strip().eq("")
    )
    return df[need_mask].copy()


# =========================
# 서비스
# =========================
class TagService:
    def __init__(self, model_dir=DEFAULT_MODEL_DIR, output_dir=DEFAULT_OUTPUT_DIR):
        self.model_dir = os.path.abspath(model_dir)
        self.output_dir = os.path.abspath(output_dir)

        if not os.path.exists(self.model_dir):
            raise FileNotFoundError(f"모델 폴더가 없습니다: {self.model_dir}")

        self.predictor = HierarchicalPredictor(self.model_dir)

    def _build_output_path(self, input_csv_path: str) -> str:
        file_name = os.path.basename(input_csv_path)
        return os.path.join(self.output_dir, file_name)

    def tag_csv(self, input_csv_paths: List[str]) -> List[str]:
        output_csv_paths = []
        for input_csv_path in input_csv_paths:
            input_csv_path = os.path.abspath(input_csv_path)
            output_csv_path = self._build_output_path(input_csv_path)
            output_csv_paths.append(output_csv_path)

            if not os.path.exists(input_csv_path):
                raise FileNotFoundError(f"입력 파일이 없습니다: {input_csv_path}")

            os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

            print(f"입력 파일 로드: {input_csv_path}")
            input_df = safe_read_csv(input_csv_path)
            validate_input_columns(input_df)

            input_df["title"] = input_df["title"].apply(normalize_text)
            input_df["content"] = input_df["content"].apply(normalize_text)

            if os.path.exists(output_csv_path):
                print(f"기존 결과 파일 발견: {output_csv_path}")
                output_df = safe_read_csv(output_csv_path)
                base_df = merge_existing_predictions(input_df, output_df)
            else:
                print("기존 결과 파일 없음. 새로 생성합니다.")
                base_df = ensure_output_columns(input_df.copy())

            base_df = ensure_output_columns(base_df)
            target_df = find_rows_to_predict(base_df)

            if len(target_df) == 0:
                print("이미 모든 행에 예측값이 채워져 있습니다.")
                base_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
                print(f"결과 파일 유지 완료: {output_csv_path}")
                continue

            texts = [
                self.predictor.build_text(title=row["title"], content=row["content"])
                for _, row in target_df.iterrows()
            ]
            target_indices = target_df.index.tolist()

            print(f"총 {len(base_df)}행 중 {len(target_df)}행 예측 진행")

            results = self.predictor.predict(texts)

            for idx, result in zip(target_indices, results):
                base_df.at[idx, "pred_major"] = result["pred_major"]
                base_df.at[idx, "pred_sub"] = result["pred_sub"]
                base_df.at[idx, "pred_label"] = result["pred_label"]
                base_df.at[idx, "pred_major_prob"] = float(result["pred_major_prob"])
                base_df.at[idx, "pred_sub_prob"] = float(result["pred_sub_prob"])

            base_df = ensure_output_columns(base_df)
            base_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
            print(f"예측 결과 저장 완료: {output_csv_path}")

        return output_csv_paths